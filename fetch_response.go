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

// Decode payload stored in byte array to FetchResponse object
func (fetchResponse *FetchResponse) Decode(payload []byte) error {
	payloadLength := uint64(len(payload))
	glog.V(5).Infof("start to decode fetch response playload, length: %d", payloadLength)
	glog.V(20).Infof("playload %v", payload)
	var offset uint64 = 0

	responseLength := uint64(binary.BigEndian.Uint32(payload))
	glog.V(10).Infof("response length: %d", responseLength)
	if responseLength+4 != payloadLength {
		//TODO lenght does not match
		glog.Error("response length NOT match")
	}
	offset += 4

	fetchResponse.CorrelationID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	responsesCount := binary.BigEndian.Uint32(payload[offset:])
	offset += 4
	glog.V(10).Infof("there is %d topic in fetch response", responsesCount)

	fetchResponse.Responses = make([]struct {
		TopicName          string
		PartitionResponses []PartitionResponse
	}, responsesCount)

	for i := uint32(0); i < responsesCount; i++ {
		topicNameLength := binary.BigEndian.Uint16(payload[offset:])
		offset += 2
		fetchResponse.Responses[i].TopicName = string(payload[offset : offset+uint64(topicNameLength)])
		offset += uint64(topicNameLength)
		glog.V(10).Infof("now process topic %s", fetchResponse.Responses[i].TopicName)

		partitionResponseCount := binary.BigEndian.Uint32(payload[offset:])
		glog.V(10).Infof("there is %d PartitionResponse", partitionResponseCount)
		offset += 4
		fetchResponse.Responses[i].PartitionResponses = make([]PartitionResponse, partitionResponseCount)
		for j := uint32(0); j < partitionResponseCount; j++ {
			glog.V(10).Infof("now process partitionResponse %d", j)
			fetchResponse.Responses[i].PartitionResponses[j].Partition = int32(binary.BigEndian.Uint32(payload[offset:]))
			glog.V(10).Infof("partition id is %d", fetchResponse.Responses[i].PartitionResponses[j].Partition)
			offset += 4
			fetchResponse.Responses[i].PartitionResponses[j].ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
			if fetchResponse.Responses[i].PartitionResponses[j].ErrorCode != 0 {
				glog.V(10).Infof("errorcode is %d", fetchResponse.Responses[i].PartitionResponses[j].ErrorCode)
				return getErrorFromErrorCode(fetchResponse.Responses[i].PartitionResponses[j].ErrorCode)
			}
			offset += 2
			fetchResponse.Responses[i].PartitionResponses[j].HighwaterMarkOffset = int64(binary.BigEndian.Uint64(payload[offset:]))
			glog.V(10).Infof("HighwaterMarkOffset id is %d", fetchResponse.Responses[i].PartitionResponses[j].HighwaterMarkOffset)
			offset += 8
			fetchResponse.Responses[i].PartitionResponses[j].MessageSetSizeBytes = int32(binary.BigEndian.Uint32(payload[offset:]))
			glog.V(10).Infof("there is %d bytes in messagesets", fetchResponse.Responses[i].PartitionResponses[j].MessageSetSizeBytes)
			offset += 4

			if messageSet, _, err := DecodeToMessageSet(payload[offset:]); err != nil {
				return err
			} else {
				fetchResponse.Responses[i].PartitionResponses[j].MessageSet = messageSet
			}
		}
	}
	return nil
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
		glog.V(10).Infof("read totally %d bytes in fetch response payload", streamDecoder.length)
	} else {
		glog.V(10).Infof("fetch response buffers closed")
	}

	glog.V(10).Infof("%p %d %d", streamDecoder, streamDecoder.length, len(streamDecoder.payload))
}

func (streamDecoder *FetchResponseStreamDecoder) encodeMessageSet(topicName string, partitionID int32, messageSetSizeBytes int32) error {
	glog.V(10).Infof("encodeMessageSet %d %d %d %d", streamDecoder.length, streamDecoder.offset, partitionID, messageSetSizeBytes)

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

		glog.V(10).Infof("offset %d length %d", streamDecoder.offset, streamDecoder.length)
		messageOffset = int64(binary.BigEndian.Uint64(streamDecoder.payload[streamDecoder.offset:]))
		glog.V(10).Infof("message offset: %d", messageOffset)
		streamDecoder.offset += 8

		messageSize = int32(binary.BigEndian.Uint32(streamDecoder.payload[streamDecoder.offset:]))
		glog.V(10).Infof("message size: %d", messageSize)
		streamDecoder.offset += 4

		for streamDecoder.more && streamDecoder.offset+int(messageSize) > streamDecoder.length {
			streamDecoder.read()
		}

		if streamDecoder.offset+int(messageSize) > streamDecoder.length {
			return &fetchResponseTruncatedBeforeMessageSet
		}

		glog.V(10).Infof("messageSize:%d offset:%d length:%d", messageSize, streamDecoder.offset, streamDecoder.length)

		messageSet, _offset, err := DecodeToMessageSet(streamDecoder.payload[streamDecoder.offset-12 : streamDecoder.offset+int(messageSize)])

		glog.V(10).Infof("messageSize:%d _offset:%d messageSet.Size:%d err:%v", messageSize, _offset, len(messageSet), err)

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
		glog.V(10).Infof("offset %d originOffset %d messageSetSizeBytes %d", streamDecoder.offset, originOffset, messageSetSizeBytes)
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
	glog.V(10).Infof("partition: %d", partition)
	streamDecoder.offset += 4

	errorCode = int16(binary.BigEndian.Uint16(streamDecoder.payload[streamDecoder.offset:]))
	glog.V(10).Infof("errorCode: %d", errorCode)
	if errorCode != 0 {
		err = getErrorFromErrorCode(errorCode)
	}
	streamDecoder.offset += 2

	highwaterMarkOffset = int64(binary.BigEndian.Uint64(streamDecoder.payload[streamDecoder.offset:]))
	glog.V(10).Infof("highwaterMarkOffset: %d", highwaterMarkOffset)
	streamDecoder.offset += 8

	messageSetSizeBytes = int32(binary.BigEndian.Uint32(streamDecoder.payload[streamDecoder.offset:]))
	glog.V(10).Infof("messageSetSizeBytes: %d", messageSetSizeBytes)
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
			glog.V(10).Infof("topicNameLength: %d", topicNameLength)
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

	glog.V(10).Infof("more: %v, topicNameLength: %d, offset: %d, length: %d", streamDecoder.more, topicNameLength, streamDecoder.offset, streamDecoder.length)
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
		glog.V(10).Infof("more %v offset %d length %d err %v", streamDecoder.more, streamDecoder.offset, streamDecoder.length, err)
		if err != nil {
			return err
		}
	}
	return nil
}

func (streamDecoder *FetchResponseStreamDecoder) consumeFetchResponse() {
	defer func() {
		glog.V(10).Info("consumeFetchResponse return")
		close(streamDecoder.messages)
	}()

	payloadLengthBuf := make([]byte, 0)
	for {
		buffer := <-streamDecoder.buffers
		glog.V(20).Infof("%p: %v", streamDecoder, buffer)
		streamDecoder.length += len(buffer)
		glog.V(10).Infof("%p: %d bytes in fetch response payload", streamDecoder, streamDecoder.length)
		payloadLengthBuf := append(payloadLengthBuf, buffer...)
		if len(payloadLengthBuf) >= 4 {
			responseLength := binary.BigEndian.Uint32(payloadLengthBuf)
			streamDecoder.totalLength = int(responseLength) + 4
			glog.V(10).Infof("%p: responseLength: %d", streamDecoder, responseLength)
			streamDecoder.payload = make([]byte, responseLength+4)
			glog.V(10).Infof("%p: payload length: %d", streamDecoder, len(streamDecoder.payload))
			copy(streamDecoder.payload, payloadLengthBuf)
			break
		}
	}
	glog.V(10).Infof("%p %d %d", streamDecoder, streamDecoder.length, len(streamDecoder.payload))

	// header
	for streamDecoder.more && streamDecoder.length < 12 {
		streamDecoder.read()
	}

	if streamDecoder.length < 12 {
		glog.Fatal("NOT get enough data in fetch response")
	}

	glog.V(10).Infof("%d, %d", streamDecoder.length, len(streamDecoder.payload))
	glog.V(20).Infof("%d: %v", streamDecoder.length, streamDecoder.payload[:streamDecoder.length])
	correlationID := binary.BigEndian.Uint32(streamDecoder.payload[4:])
	glog.V(10).Infof("correlationID: %d", correlationID)

	responsesCount := binary.BigEndian.Uint32(streamDecoder.payload[8:])
	streamDecoder.offset = 12

	glog.V(10).Infof("responsesCount: %d", responsesCount)
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
