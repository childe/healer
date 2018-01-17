package healer

import (
	"encoding/binary"

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
			glog.V(20).Infof("read totally %d bytes in fetch response payload", streamDecoder.length)
		} else {
			glog.V(20).Info("fetch response buffers closed")
			return rst, length
		}
	}
	streamDecoder.depositBuffer = buffer[i:]

	return rst, length
}

func (streamDecoder *FetchResponseStreamDecoder) encodeMessageSet(topicName string, partitionID int32, messageSetSizeBytes int32) error {
	var (
		messageOffset int64
		messageSize   int32

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
		buffer, _ = streamDecoder.read(8)
		copy(value, buffer)

		messageOffset = int64(binary.BigEndian.Uint64(buffer))
		offset += 8
		glog.V(18).Infof("message offset: %d", messageOffset)

		buffer, _ = streamDecoder.read(4)
		copy(value[8:], buffer)
		messageSize = int32(binary.BigEndian.Uint32(buffer))
		glog.V(18).Infof("message size: %d", messageSize)
		offset += 4

		buffer, n = streamDecoder.read(int(messageSize))
		// TODO remove memory copy
		value = append(value, buffer...)

		if n < int(messageSize) {
			if !hasAtLeastOneMessage {
				return &maxBytesTooSmall
			}
			return nil
		}
		offset += messageSize

		messageSet, err := DecodeToMessageSet(value)

		glog.V(20).Infof("messageSet.Size:%d err:%v", len(messageSet), err)

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
		partition           int32
		errorCode           int16
		highwaterMarkOffset int64
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
	glog.V(20).Infof("partition: %d", partition)

	errorCode = int16(binary.BigEndian.Uint16(buffer[4:]))
	glog.V(20).Infof("errorCode: %d", errorCode)
	if errorCode != 0 {
		err = getErrorFromErrorCode(errorCode)
	}

	highwaterMarkOffset = int64(binary.BigEndian.Uint64(buffer[6:]))
	glog.V(20).Infof("highwaterMarkOffset: %d", highwaterMarkOffset)

	messageSetSizeBytes = int32(binary.BigEndian.Uint32((buffer[14:])))
	glog.V(20).Infof("messageSetSizeBytes: %d", messageSetSizeBytes)

	if err != nil {
		return err
	}
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
	glog.V(18).Infof("topicNameLength: %d", topicNameLength)

	buffer, _ = streamDecoder.read(topicNameLength)
	topicName := string(buffer)
	glog.V(20).Infof("topicName: %s", topicName)

	buffer, _ = streamDecoder.read(4)
	partitionResponseCount := binary.BigEndian.Uint32(buffer)
	glog.V(18).Infof("partitionResponseCount: %d", partitionResponseCount)

	if partitionResponseCount == 0 {
		return &noPartitionResponse
	}

	for ; partitionResponseCount > 0; partitionResponseCount-- {
		err = streamDecoder.encodePartitionResponse(topicName)
		glog.V(20).Infof("more %v length %d err %v", streamDecoder.more, streamDecoder.length, err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (streamDecoder *FetchResponseStreamDecoder) consumeFetchResponse() {
	defer func() {
		glog.V(20).Info("consumeFetchResponse return")
		close(streamDecoder.messages)
	}()

	streamDecoder.depositBuffer = make([]byte, 0)

	payloadLengthBuf, n := streamDecoder.read(4)
	if n != 4 {
		glog.Error("could read enough bytes(4) from buffer channel")
		return
	}
	responseLength := binary.BigEndian.Uint32(payloadLengthBuf)
	streamDecoder.totalLength = int(responseLength) + 4
	glog.V(20).Infof("%p: totalLength: %d", streamDecoder, streamDecoder.totalLength)

	// header
	buffer, n := streamDecoder.read(8)
	if n != 8 {
		glog.Error("could read enough bytes(8) from buffer channel for fetch response header")
	}

	correlationID := binary.BigEndian.Uint32(buffer)
	glog.V(20).Infof("correlationID: %d", correlationID)

	responsesCount := binary.BigEndian.Uint32(buffer[4:])

	glog.V(20).Infof("responsesCount: %d", responsesCount)
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
