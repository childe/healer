package healer

import (
	"encoding/binary"

	"github.com/childe/glog"
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
	length        int
	buffers       chan []byte
	messages      chan *FullMessage
	more          bool
}

func (streamDecoder *FetchResponseStreamDecoder) read(n int) ([]byte, int) {
	var (
		rst    []byte = make([]byte, n)
		length int    = 0

		buffer []byte
		i      int
	)
	if len(streamDecoder.depositBuffer) >= n {
		length = copy(rst, streamDecoder.depositBuffer)
		streamDecoder.depositBuffer = streamDecoder.depositBuffer[length:]
		return rst, length
	}
	length = copy(rst, streamDecoder.depositBuffer)

	for length < n {
		buffer, streamDecoder.more = <-streamDecoder.buffers
		if streamDecoder.more {
			i = copy(rst[length:], buffer)
			length += i
			streamDecoder.length += len(buffer)
		} else {
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

		buffer []byte
		n      int
		offset int32 = 0
	)

	hasAtLeastOneMessage := false
	for {
		if offset == messageSetSizeBytes {
			return nil
		}
		value := make([]byte, 12)
		buffer, n = streamDecoder.read(8)
		if n < 8 {
			if !hasAtLeastOneMessage {
				return &maxBytesTooSmall
			}
			return nil
		}
		copy(value, buffer)

		//messageOffset = int64(binary.BigEndian.Uint64(buffer))
		offset += 8

		buffer, n = streamDecoder.read(4)
		if n < 4 {
			if !hasAtLeastOneMessage {
				return &maxBytesTooSmall
			}
			return nil
		}
		copy(value[8:], buffer)
		messageSize = int32(binary.BigEndian.Uint32(buffer))
		if messageSize <= 0 {
			return nil
		}
		offset += 4

		buffer, n = streamDecoder.read(int(messageSize))

		if n < int(messageSize) {
			if !hasAtLeastOneMessage {
				return &maxBytesTooSmall
			}
			return nil
		}
		// TODO remove memory copy
		value = append(value, buffer...)

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

	return nil
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

func (streamDecoder *FetchResponseStreamDecoder) consumeFetchResponse() {
	defer func() {
		close(streamDecoder.messages)
	}()

	streamDecoder.depositBuffer = make([]byte, 0)

	payloadLengthBuf, n := streamDecoder.read(4)
	if n != 4 {
		glog.Errorf("could read enough bytes(4) to get fetchresponse length. read %d bytes", n)
		return
	}
	responseLength := binary.BigEndian.Uint32(payloadLengthBuf)
	streamDecoder.totalLength = int(responseLength) + 4

	// header
	buffer, n := streamDecoder.read(8)
	if n != 8 {
		glog.Errorf("could read enough bytes(8) from buffer channel for fetch response header. read %d bytes", n)
		return
	}

	//correlationID := binary.BigEndian.Uint32(buffer)

	responsesCount := binary.BigEndian.Uint32(buffer[4:])

	if responsesCount == 0 {
		return
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
}
