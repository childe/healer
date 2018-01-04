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
	payload     []byte
	totalLength int
	length      int
	offset      int
	buffers     chan []byte
	messages    chan *FullMessage
	more        bool
}

func (streamDecoder *FetchResponseStreamDecoder) read() {
	var buffer []byte
	buffer, streamDecoder.more = <-streamDecoder.buffers
	if streamDecoder.more {
		copy(streamDecoder.payload[streamDecoder.length:], buffer)
		streamDecoder.length += len(buffer)
		glog.V(20).Infof("read totally %d bytes in fetch response payload", streamDecoder.length)
	} else {
		glog.V(20).Infof("fetch response buffers closed")
	}

	glog.V(20).Infof("%p length:%d payload length:%d", streamDecoder, streamDecoder.length, len(streamDecoder.payload))
}

func (streamDecoder *FetchResponseStreamDecoder) encodeMessageSet(topicName string, partitionID int32, messageSetSizeBytes int32) error {
	glog.V(20).Infof("encodeMessageSet %d %d %d %d", streamDecoder.length, streamDecoder.offset, partitionID, messageSetSizeBytes)

	var messageOffset int64
	var messageSize int32
	var originOffset int = streamDecoder.offset

	for streamDecoder.offset-originOffset < int(messageSetSizeBytes) {
		for streamDecoder.more && streamDecoder.offset+12 > streamDecoder.length {
			streamDecoder.read()
		}

		if streamDecoder.offset+12 > streamDecoder.length {
			return &fetchResponseTruncatedBeforeMessageSet
		}

		glog.V(20).Infof("offset %d length %d", streamDecoder.offset, streamDecoder.length)
		messageOffset = int64(binary.BigEndian.Uint64(streamDecoder.payload[streamDecoder.offset:]))
		glog.V(20).Infof("message offset: %d", messageOffset)
		streamDecoder.offset += 8

		messageSize = int32(binary.BigEndian.Uint32(streamDecoder.payload[streamDecoder.offset:]))
		glog.V(20).Infof("message size: %d", messageSize)
		streamDecoder.offset += 4

		for streamDecoder.more && streamDecoder.offset+int(messageSize) > streamDecoder.length {
			streamDecoder.read()
		}

		if streamDecoder.offset+int(messageSize) > streamDecoder.length {
			return &fetchResponseTruncatedBeforeMessageSet
		}

		glog.V(20).Infof("messageSize:%d offset:%d length:%d", messageSize, streamDecoder.offset, streamDecoder.length)

		messageSet, _offset, err := DecodeToMessageSet(streamDecoder.payload[streamDecoder.offset-12 : streamDecoder.offset+int(messageSize)])

		glog.V(20).Infof("messageSize:%d _offset:%d messageSet.Size:%d err:%v", messageSize, _offset, len(messageSet), err)

		if err != nil {
			return err
		} else {
			streamDecoder.offset += _offset - 12
			for i := range messageSet {
				streamDecoder.messages <- &FullMessage{
					TopicName:   topicName,
					PartitionID: partitionID,
					Message:     messageSet[i],
				}
			}
		}
		glog.V(20).Infof("offset %d originOffset %d messageSetSizeBytes %d", streamDecoder.offset, originOffset, messageSetSizeBytes)
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
	)

	for streamDecoder.offset+18 > streamDecoder.length && streamDecoder.more {
		streamDecoder.read()
	}

	if streamDecoder.offset+18 > streamDecoder.length {
		return &fetchResponseTruncatedBeforeMessageSet
	}

	partition = int32(binary.BigEndian.Uint32(streamDecoder.payload[streamDecoder.offset:]))
	glog.V(20).Infof("partition: %d", partition)
	streamDecoder.offset += 4

	errorCode = int16(binary.BigEndian.Uint16(streamDecoder.payload[streamDecoder.offset:]))
	glog.V(20).Infof("errorCode: %d", errorCode)
	if errorCode != 0 {
		err = getErrorFromErrorCode(errorCode)
	}
	streamDecoder.offset += 2

	highwaterMarkOffset = int64(binary.BigEndian.Uint64(streamDecoder.payload[streamDecoder.offset:]))
	glog.V(20).Infof("highwaterMarkOffset: %d", highwaterMarkOffset)
	streamDecoder.offset += 8

	messageSetSizeBytes = int32(binary.BigEndian.Uint32(streamDecoder.payload[streamDecoder.offset:]))
	glog.V(20).Infof("messageSetSizeBytes: %d", messageSetSizeBytes)
	streamDecoder.offset += 4

	if err != nil {
		return err
	}
	err = streamDecoder.encodeMessageSet(topicName, partition, messageSetSizeBytes)
	return err
}

func (streamDecoder *FetchResponseStreamDecoder) encodeResponses() error {
	var (
		topicName       string = ""
		topicNameLength int    = -1
		err             error
	)

	for {
		streamDecoder.read()
		if topicNameLength == -1 {
			if streamDecoder.offset+2 > streamDecoder.length {
				continue
			}
			topicNameLength = int(binary.BigEndian.Uint16(streamDecoder.payload[streamDecoder.offset:]))
			glog.V(20).Infof("topicNameLength: %d", topicNameLength)
			streamDecoder.offset += 2
		}
		if topicName == "" {
			if streamDecoder.offset+topicNameLength > streamDecoder.length {
				continue
			}
			topicName = string(streamDecoder.payload[streamDecoder.offset : streamDecoder.offset+topicNameLength])
			glog.V(15).Infof("topicName: %s", topicName)
			streamDecoder.offset += topicNameLength
			break
		}
	}

	glog.V(20).Infof("more: %v, topicNameLength: %d, offset: %d, length: %d", streamDecoder.more, topicNameLength, streamDecoder.offset, streamDecoder.length)
	glog.V(15).Infof("toppic name: %s", topicName)

	var partitionResponseCount uint32
	for streamDecoder.more && streamDecoder.offset+4 > streamDecoder.length {
		streamDecoder.read()
	}

	if streamDecoder.offset+4 > streamDecoder.length {
		return &fetchResponseTruncatedBeforeMessageSet
	}

	partitionResponseCount = binary.BigEndian.Uint32(streamDecoder.payload[streamDecoder.offset:])
	streamDecoder.offset += 4

	if partitionResponseCount == 0 {
		return &noPartitionResponse
	}

	for ; partitionResponseCount > 0; partitionResponseCount-- {
		err = streamDecoder.encodePartitionResponse(topicName)
		glog.V(20).Infof("more %v offset %d length %d err %v", streamDecoder.more, streamDecoder.offset, streamDecoder.length, err)
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

	payloadLengthBuf := make([]byte, 0)
	for {
		buffer := <-streamDecoder.buffers
		glog.V(20).Infof("%p: %v", streamDecoder, buffer)
		streamDecoder.length += len(buffer)
		glog.V(20).Infof("%p: %d bytes in fetch response payload", streamDecoder, streamDecoder.length)
		payloadLengthBuf := append(payloadLengthBuf, buffer...)
		if len(payloadLengthBuf) >= 4 {
			responseLength := binary.BigEndian.Uint32(payloadLengthBuf)
			streamDecoder.totalLength = int(responseLength) + 4
			glog.V(20).Infof("%p: responseLength: %d", streamDecoder, responseLength)
			streamDecoder.payload = make([]byte, responseLength+4)
			glog.V(20).Infof("%p: payload length: %d", streamDecoder, len(streamDecoder.payload))
			copy(streamDecoder.payload, payloadLengthBuf)
			break
		}
	}
	glog.V(20).Infof("%p %d %d", streamDecoder, streamDecoder.length, len(streamDecoder.payload))

	// header
	for streamDecoder.more && streamDecoder.length < 12 {
		streamDecoder.read()
	}

	if streamDecoder.length < 12 {
		glog.Fatal("NOT get enough data in fetch response")
	}

	glog.V(20).Infof("%d, %d", streamDecoder.length, len(streamDecoder.payload))
	glog.V(20).Infof("%d: %v", streamDecoder.length, streamDecoder.payload[:streamDecoder.length])
	correlationID := binary.BigEndian.Uint32(streamDecoder.payload[4:])
	glog.V(20).Infof("correlationID: %d", correlationID)

	responsesCount := binary.BigEndian.Uint32(streamDecoder.payload[8:])
	streamDecoder.offset = 12

	glog.V(20).Infof("responsesCount: %d", responsesCount)
	if responsesCount == 0 {
		return
	}
	for ; responsesCount > 0; responsesCount-- {
		err := streamDecoder.encodeResponses()
		if err != nil && err != &fetchResponseTruncatedInMessageSet {
			streamDecoder.messages <- &FullMessage{
				TopicName:   "",
				PartitionID: -1,
				Error:       err,
				Message:     nil,
			}
			// TODO could not fatal
			glog.Error(err)
		}
	}
}
