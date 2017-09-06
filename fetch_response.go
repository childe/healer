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
	CorrelationId int32
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

	fetchResponse.CorrelationId = int32(binary.BigEndian.Uint32(payload[offset:]))
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
				return AllError[fetchResponse.Responses[i].PartitionResponses[j].ErrorCode]
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

func (streamDecoder *FetchResponseStreamDecoder) encodeMessageSet(topicName string, partitionID int32, messageSetSizeBytes int32) error {
	glog.V(10).Infof("encodeMessageSet %d %d %d %d", streamDecoder.length, streamDecoder.offset, partitionID, messageSetSizeBytes)
	//Offset      int64
	//MessageSize int32

	////Message
	//Crc        uint32
	//MagicByte  int8
	//Attributes int8
	//Key        []byte
	//Value      []byte
	var messageOffset int64
	var messageSize int32
	var originOffset int = streamDecoder.offset

	var buffer []byte
	for {
		for {
			buffer, streamDecoder.more = <-streamDecoder.buffers
			if streamDecoder.more {
				copy(streamDecoder.payload[streamDecoder.length:], buffer)
				streamDecoder.length += len(buffer)
				glog.V(10).Infof("%d bytes in fetch response payload", streamDecoder.length)
			} else {
				glog.V(10).Infof("fetch response buffer chan closed. length %d", streamDecoder.length)
				break
			}

			if streamDecoder.offset+12 > streamDecoder.length {
				continue
			} else {
				break
			}
		}

		if streamDecoder.offset+12 > streamDecoder.length {
			return &fetchResponseTruncated
		}

		glog.V(10).Infof("offset %d length %d", streamDecoder.offset, streamDecoder.length)
		messageOffset = int64(binary.BigEndian.Uint64(streamDecoder.payload[streamDecoder.offset:]))
		glog.V(10).Infof("message offset: %d", messageOffset)
		streamDecoder.offset += 8

		messageSize = int32(binary.BigEndian.Uint32(streamDecoder.payload[streamDecoder.offset:]))
		glog.V(10).Infof("message size: %d", messageSize)
		streamDecoder.offset += 4

		var buffer []byte
		for {
			buffer, streamDecoder.more = <-streamDecoder.buffers
			if streamDecoder.more {
				copy(streamDecoder.payload[streamDecoder.length:], buffer)
				streamDecoder.length += len(buffer)
				glog.V(10).Infof("%d bytes in fetch response payload", streamDecoder.length)
			} else {
				glog.V(10).Infof("fetch response buffer chan closed. length %d", streamDecoder.length)
				break
			}

			if streamDecoder.offset+int(messageSize) > streamDecoder.length {
				continue
			} else {
				break
			}
		}

		if streamDecoder.offset+int(messageSize) > streamDecoder.length {
			return &fetchResponseTruncated
		}

		glog.V(10).Infof("messageSize:%d offset:%d length:%d", messageSize, streamDecoder.offset, streamDecoder.length)

		messageSet, _offset, err := DecodeToMessageSet(streamDecoder.payload[streamDecoder.offset-12 : streamDecoder.offset+int(messageSize)])

		glog.V(10).Infof("messageSize:%d _offset:%d messageSet.Size:%d err:%v", messageSize, _offset, len(messageSet), err)

		if err != nil {
			if err == &fetchResponseTruncated {
				for i := range messageSet {
					streamDecoder.messages <- &FullMessage{
						TopicName:   topicName,
						PartitionID: partitionID,
						Message:     messageSet[i],
					}
				}
				return err
			} else {
				return err
			}
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
		if streamDecoder.offset-originOffset >= int(messageSetSizeBytes) {
			break
		}
	}

	return nil
}

// TODO refactor
func (streamDecoder *FetchResponseStreamDecoder) encodePartitionResponse(topicName string) error {
	var (
		partition           int32 = -1
		errorCode           int16 = -1
		highwaterMarkOffset int64 = -1
		messageSetSizeBytes int32 = -1
		buffer              []byte
		err                 error
	)

	for {
		if !streamDecoder.more {
			return nil
		}
		buffer, streamDecoder.more = <-streamDecoder.buffers

		if streamDecoder.more {
			copy(streamDecoder.payload[streamDecoder.length:], buffer)
			streamDecoder.length += len(buffer)
			glog.V(10).Infof("%d bytes in fetch response payload", streamDecoder.length)
		}

		if partition == -1 {
			if streamDecoder.offset+4 > streamDecoder.length {
				continue
			}
			partition = int32(binary.BigEndian.Uint32(streamDecoder.payload[streamDecoder.offset:]))
			glog.V(10).Infof("partition: %d", partition)
			streamDecoder.offset += 4
		}

		if errorCode == -1 {
			if streamDecoder.offset+2 > streamDecoder.length {
				continue
			}
			errorCode = int16(binary.BigEndian.Uint16(streamDecoder.payload[streamDecoder.offset:]))
			glog.V(10).Infof("errorCode: %d", errorCode)
			streamDecoder.offset += 2
		}

		if highwaterMarkOffset == -1 {
			if streamDecoder.offset+8 > streamDecoder.length {
				continue
			}
			highwaterMarkOffset = int64(binary.BigEndian.Uint64(streamDecoder.payload[streamDecoder.offset:]))
			glog.V(10).Infof("highwaterMarkOffset: %d", highwaterMarkOffset)
			streamDecoder.offset += 8
		}

		if messageSetSizeBytes == -1 {
			if streamDecoder.offset+4 > streamDecoder.length {
				continue
			}
			messageSetSizeBytes = int32(binary.BigEndian.Uint32(streamDecoder.payload[streamDecoder.offset:]))
			glog.V(10).Infof("messageSetSizeBytes: %d", messageSetSizeBytes)
			streamDecoder.offset += 4
		}

		err = streamDecoder.encodeMessageSet(topicName, partition, messageSetSizeBytes)
		if err != nil {
			return err
		}
	}
}

func (streamDecoder *FetchResponseStreamDecoder) encodePartitionResponses(topicName string) error {
	var (
		partitionResponseCount int = -1
		counter                int = 0

		buffer []byte
		err    error
	)

	for {
		buffer, streamDecoder.more = <-streamDecoder.buffers
		if streamDecoder.more {
			copy(streamDecoder.payload[streamDecoder.length:], buffer)
			streamDecoder.length += len(buffer)
			glog.V(10).Infof("%d bytes in fetch response payload", streamDecoder.length)
		}

		if partitionResponseCount == -1 {
			if streamDecoder.offset+4 > streamDecoder.length {
				continue
			}
			partitionResponseCount = int(binary.BigEndian.Uint32(streamDecoder.payload[streamDecoder.offset:]))
			streamDecoder.offset += 4

			if partitionResponseCount == 0 {
				return nil
			}
		}

		err = streamDecoder.encodePartitionResponse(topicName)
		if err != nil {
			return err
		}
		counter++

		if counter == partitionResponseCount {
			return nil
		}
	}
}

func (streamDecoder *FetchResponseStreamDecoder) encodeResponses() error {
	var (
		topicName       string = ""
		topicNameLength int    = -1
		err             error
		buffer          []byte
	)

	for {
		if !streamDecoder.more {
			return &notEnoughDataInFetchResponse
		}

		buffer, streamDecoder.more = <-streamDecoder.buffers
		glog.V(10).Info(streamDecoder.more)
		if streamDecoder.more {
			copy(streamDecoder.payload[streamDecoder.length:], buffer)
			streamDecoder.length += len(buffer)
			glog.V(10).Infof("%d bytes in fetch response payload", streamDecoder.length)
		}

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
		}

		glog.V(10).Infof("more: %v, topicNameLength: %d, offset: %d, length: %d", streamDecoder.more, topicNameLength, streamDecoder.offset, streamDecoder.length)
		glog.V(15).Infof("toppic name: %s", topicName)

		err = streamDecoder.encodePartitionResponses(topicName)
		glog.V(10).Infof("more %v offset %d length %d err %v", streamDecoder.more, streamDecoder.offset, streamDecoder.length, err)
		if err != nil {
			return err
		}
		if !streamDecoder.more {
			return nil
		}

		topicName = ""
		topicNameLength = -1
	}
}

func (streamDecoder *FetchResponseStreamDecoder) consumeFetchResponse() {
	defer func() {
		glog.V(10).Info("consumeFetchResponse return")
		close(streamDecoder.messages)
	}()

	payloadLengthBuf := make([]byte, 0)
	for {
		buffer := <-streamDecoder.buffers
		glog.V(20).Infof("%v", buffer)
		streamDecoder.length += len(buffer)
		glog.V(10).Infof("%d bytes in fetch response payload", streamDecoder.length)
		payloadLengthBuf := append(payloadLengthBuf, buffer...)
		if len(payloadLengthBuf) >= 4 {
			responseLength := binary.BigEndian.Uint32(payloadLengthBuf)
			streamDecoder.totalLength = int(responseLength) + 4
			glog.V(10).Infof("responseLength: %d", responseLength)
			streamDecoder.payload = make([]byte, responseLength+4)
			copy(streamDecoder.payload, payloadLengthBuf)
			break
		}
	}

	// header
	var buffer []byte
	for {
		buffer, streamDecoder.more = <-streamDecoder.buffers
		if streamDecoder.more {
			glog.V(20).Infof("%v", buffer)
			copy(streamDecoder.payload[streamDecoder.length:], buffer)
			streamDecoder.length += len(buffer)
			glog.V(10).Infof("%d bytes in fetch response payload", streamDecoder.length)
			if streamDecoder.length >= 12 {
				break
			}
		} else {
			glog.Fatal("NOT get enough data to build FetchResponse")
			return
		}
	}

	glog.V(20).Infof("%d: %v", streamDecoder.length, streamDecoder.payload[:streamDecoder.length])
	correlationID := binary.BigEndian.Uint32(streamDecoder.payload[4:])
	glog.V(10).Infof("correlationID: %d", correlationID)

	responsesCount := binary.BigEndian.Uint32(streamDecoder.payload[8:])
	glog.V(10).Infof("responsesCount: %d", responsesCount)
	if responsesCount == 0 {
		return
	}
	streamDecoder.offset = 12
	err := streamDecoder.encodeResponses()
	if err != nil && err != &fetchResponseTruncated {
		glog.Fatal(err)
	}
}
