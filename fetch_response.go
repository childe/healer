package healer

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/golang/glog"
)

/*
FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
  TopicName => string
  Partition => int32
  ErrorCode => int16
  HighwaterMarkOffset => int64
  MessageSetSizeBytes => int32

Field					Description
HighwaterMarkOffset		The offset at the end of the log for this partition. This can be used by the client to determine how many messages behind the end of the log they are.
MessageSet				The message data fetched from this partition, in the format described above.
MessageSetSizeBytes			The size in bytes of the message set for this partition
Partition				The id of the partition this response is for.
TopicName				The name of the topic this response entry is for.
*/

// PartitionResponse stores partitionID and MessageSet in the partition
type PartitionResponse struct {
	Partition           int32
	ErrorCode           int16
	HighwaterMarkOffset int64
	MessageSetSizeBytes int32
	MessageSet          MessageSet
}

// FetchResponse stores topicname and arrya of PartitionResponse
type FetchResponse struct {
	CorrelationID int32
	Responses     []struct {
		TopicName          string
		PartitionResponses []PartitionResponse
	}
}

type FetchResponseStreamDecoder struct {
	depositBuffer []byte
	totalLength   int
	//length        int
	buffers  chan []byte
	messages chan *FullMessage
	more     bool
}

func (streamDecoder *FetchResponseStreamDecoder) readAll() (length int) {
	length = len(streamDecoder.depositBuffer)
	streamDecoder.depositBuffer = streamDecoder.depositBuffer[:0]

	var buffer []byte
	for {
		buffer = <-streamDecoder.buffers
		if buffer != nil {
			length += len(buffer)
			streamDecoder.more = true
		} else {
			streamDecoder.more = false
			return
		}
	}
}

var errShortRead = errors.New("short read")

func (streamDecoder *FetchResponseStreamDecoder) readToBuf(p []byte) (n int, err error) {
	var (
		l      int
		length int = len(p)
		i      int
	)
	if len(streamDecoder.depositBuffer) >= length {
		l = copy(p, streamDecoder.depositBuffer)
		if l != length {
			return l, errShortRead
		}

		streamDecoder.depositBuffer = streamDecoder.depositBuffer[length:]
		return l, nil
	}

	l = copy(p, streamDecoder.depositBuffer)

	var buffer []byte
	for l < length {
		buffer = <-streamDecoder.buffers
		if buffer != nil {
			streamDecoder.more = true
			i = copy(p[l:], buffer)
			l += i
			//streamDecoder.length += len(buffer)
		} else {
			streamDecoder.more = false
			return l, io.EOF
		}
	}
	streamDecoder.depositBuffer = buffer[i:]

	return length, nil
}

func (streamDecoder *FetchResponseStreamDecoder) read(n int) ([]byte, int) {
	if len(streamDecoder.depositBuffer) >= n {
		rst := streamDecoder.depositBuffer[:n]
		streamDecoder.depositBuffer = streamDecoder.depositBuffer[n:]
		return rst, n
	}

	var (
		rst    []byte = make([]byte, n)
		length int    = 0

		buffer []byte
		i      int
	)

	length = copy(rst, streamDecoder.depositBuffer)

	for length < n {
		buffer = <-streamDecoder.buffers
		if buffer != nil {
			streamDecoder.more = true
			i = copy(rst[length:], buffer)
			length += i
			//streamDecoder.length += len(buffer)
		} else {
			streamDecoder.more = false
			return rst, length
		}
	}
	streamDecoder.depositBuffer = buffer[i:]

	return rst, length
}

func (streamDecoder *FetchResponseStreamDecoder) encodeMessageSet(topicName string, partitionID int32, messageSetSizeBytes int32) error {
	var (
		//messageOffset int64
		messageSize int32
		n           int
		offset      int32 = 0
	)

	hasAtLeastOneMessage := false
	for {
		if offset == messageSetSizeBytes {
			return nil
		}
		buf := make([]byte, 12)
		n, _ = streamDecoder.readToBuf(buf[:8])
		if n < 8 {
			if !hasAtLeastOneMessage {
				return &maxBytesTooSmall
			}
			return nil
		}

		//messageOffset = int64(binary.BigEndian.Uint64(buffer))
		offset += 8

		n, _ = streamDecoder.readToBuf(buf[8:])
		if n < 4 {
			if !hasAtLeastOneMessage {
				return &maxBytesTooSmall
			}
			return nil
		}
		messageSize = int32(binary.BigEndian.Uint32(buf[8:12]))
		if messageSize <= 0 {
			return nil
		}
		offset += 4

		value := make([]byte, 12+int(messageSize))
		copy(value, buf)
		n, _ = streamDecoder.readToBuf(value[12:])

		if n < int(messageSize) {
			if !hasAtLeastOneMessage {
				return &maxBytesTooSmall
			}
			return nil
		}

		offset += messageSize

		messageSet, err := DecodeToMessageSet(value)

		if err != nil {
			return err
		} else {
			for i := range messageSet {
				streamDecoder.messages <- &FullMessage{
					TopicName:   topicName,
					PartitionID: partitionID,
					Message:     messageSet[i],
				}
			}
		}
		hasAtLeastOneMessage = true
	}
}

func (streamDecoder *FetchResponseStreamDecoder) encodePartitionResponse(topicName string) error {
	var (
		partition int32
		errorCode int16
		//highwaterMarkOffset int64
		messageSetSizeBytes int32
		err                 error

		buffer []byte
		n      int
	)

	buffer, n = streamDecoder.read(18)

	if n < 18 {
		return &maxBytesTooSmall
	}

	partition = int32(binary.BigEndian.Uint32(buffer))

	errorCode = int16(binary.BigEndian.Uint16(buffer[4:]))
	if errorCode != 0 {
		return getErrorFromErrorCode(errorCode)
	}

	//highwaterMarkOffset = int64(binary.BigEndian.Uint64(buffer[6:]))

	messageSetSizeBytes = int32(binary.BigEndian.Uint32((buffer[14:])))

	err = streamDecoder.encodeMessageSet(topicName, partition, messageSetSizeBytes)
	return err
}

func (streamDecoder *FetchResponseStreamDecoder) encodeResponses() error {
	var (
		err    error
		buffer []byte
	)

	buffer, _ = streamDecoder.read(2)
	topicNameLength := int(binary.BigEndian.Uint16(buffer))

	buffer, _ = streamDecoder.read(topicNameLength)
	topicName := string(buffer)

	buffer, _ = streamDecoder.read(4)
	partitionResponseCount := binary.BigEndian.Uint32(buffer)

	if partitionResponseCount == 0 {
		return &noPartitionResponse
	}

	for ; partitionResponseCount > 0; partitionResponseCount-- {
		err = streamDecoder.encodePartitionResponse(topicName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (streamDecoder *FetchResponseStreamDecoder) consumeFetchResponse() bool {
	defer func() {
		//close(streamDecoder.messages)
		streamDecoder.messages <- nil
	}()

	streamDecoder.depositBuffer = make([]byte, 0)

	payloadLengthBuf, n := streamDecoder.read(4)
	if n != 4 {
		glog.Errorf("could not read enough bytes(4) to get fetchresponse length. read %d bytes", n)
		return false
	}
	responseLength := binary.BigEndian.Uint32(payloadLengthBuf)
	streamDecoder.totalLength = int(responseLength) + 4

	// header
	buffer, n := streamDecoder.read(8)
	if n != 8 {
		glog.Errorf("could read enough bytes(8) from buffer channel for fetch response header. read %d bytes", n)
		return false
	}

	correlationID := binary.BigEndian.Uint32(buffer)
	if glog.V(10) {
		glog.Infof("fetch correlationID: %d", correlationID)
	}

	responsesCount := binary.BigEndian.Uint32(buffer[4:])

	if responsesCount == 0 {
		return true
	}
	for ; responsesCount > 0; responsesCount-- {
		err := streamDecoder.encodeResponses()
		if err != nil {
			streamDecoder.messages <- &FullMessage{
				TopicName:   "",
				PartitionID: -1,
				Error:       err,
				Message:     nil,
			}
			glog.Error(err)
		}
	}

	// 早期版本的协议可能会有多出来的字节,需要丢弃
	if streamDecoder.more {
		n = streamDecoder.readAll()
	}
	if glog.V(10) {
		glog.Infof("fetch correlationID: %d done", correlationID)
	}

	return true
}
