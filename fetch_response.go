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

func encodeMessageSet(payload []byte, length int, offset int, partition int32, messageSetSizeBytes int32, buffers chan []byte, messages chan *Message) (int, int, error) {
	glog.V(10).Infof("encodeMessageSet %d %d %d %d", length, offset, partition, messageSetSizeBytes)
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
	var originOffset int = offset

	for {
		for {
			buffer, more := <-buffers
			if more {
				copy(payload[length:], buffer)
				length += len(buffer)
				glog.V(10).Infof("%d bytes in fetch response payload", length)
			} else {
				glog.V(10).Infof("fetch response buffer chan closed. length %d", length)
				break
			}

			if offset+12 > length {
				continue
			} else {
				break
			}
		}

		if offset+12 > length {
			return offset, length, &fetchResponseTruncated
		}

		glog.V(10).Infof("offset %d length %d", offset, length)
		messageOffset = int64(binary.BigEndian.Uint64(payload[offset:]))
		glog.V(10).Infof("message offset: %d", messageOffset)
		offset += 8

		messageSize = int32(binary.BigEndian.Uint32(payload[offset:]))
		glog.V(10).Infof("message size: %d", messageSize)
		offset += 4

		for {
			buffer, more := <-buffers
			if more {
				copy(payload[length:], buffer)
				length += len(buffer)
				glog.V(10).Infof("%d bytes in fetch response payload", length)
			} else {
				glog.V(10).Infof("fetch response buffer chan closed. length %d", length)
				break
			}

			if offset+int(messageSize) > length {
				continue
			} else {
				break
			}
		}

		if offset+int(messageSize) > length {
			return offset, length, &fetchResponseTruncated
		}

		glog.V(10).Infof("messageSize:%d offset:%d length:%d", messageSize, offset, length)

		messageSet, _offset, err := DecodeToMessageSet(payload[offset-12 : offset+int(messageSize)])

		glog.V(10).Infof("messageSize:%d _offset:%d messageSet.Size:%d err:%s", messageSize, _offset, len(messageSet), err)

		if err != nil {
			if err == &fetchResponseTruncated {
				for i := range messageSet {
					messages <- messageSet[i]
				}
				return offset, length, err
			} else {
				return offset, length, err
			}
		} else {
			offset += _offset - 12
			for i := range messageSet {
				messages <- messageSet[i]
			}
		}
		glog.V(10).Infof("offset %d originOffset %d messageSetSizeBytes %d", offset, originOffset, messageSetSizeBytes)
		if offset-originOffset >= int(messageSetSizeBytes) {
			break
		}
	}

	return offset, length, nil
}

// TODO refactor
func encodePartitionResponse(payload []byte, length int, offset int, buffers chan []byte, messages chan *Message) (int, int, error) {
	glog.V(10).Info("encodePartitionResponse")
	//Partition           int32
	//ErrorCode           int16
	//HighwaterMarkOffset int64
	//MessageSetSizeBytes int32
	//MessageSet          MessageSet
	var (
		partition           int32 = -1
		errorCode           int16 = -1
		highwaterMarkOffset int64 = -1
		messageSetSizeBytes int32 = -1
		more                bool  = true
		buffer              []byte
		err                 error
	)

	for {
		if !more {
			return offset, length, nil
		}
		buffer, more = <-buffers

		if more {
			copy(payload[length:], buffer)
			length += len(buffer)
			glog.V(10).Infof("%d bytes in fetch response payload", length)
		}

		if partition == -1 {
			if offset+4 > length {
				continue
			}
			partition = int32(binary.BigEndian.Uint32(payload[offset:]))
			glog.V(10).Infof("partition: %d", partition)
			offset += 4
		}

		if errorCode == -1 {
			if offset+2 > length {
				continue
			}
			errorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
			glog.V(10).Infof("errorCode: %d", errorCode)
			offset += 2
		}

		if highwaterMarkOffset == -1 {
			if offset+8 > length {
				continue
			}
			highwaterMarkOffset = int64(binary.BigEndian.Uint64(payload[offset:]))
			glog.V(10).Infof("highwaterMarkOffset: %d", highwaterMarkOffset)
			offset += 8
		}

		if messageSetSizeBytes == -1 {
			if offset+4 > length {
				continue
			}
			messageSetSizeBytes = int32(binary.BigEndian.Uint32(payload[offset:]))
			glog.V(10).Infof("messageSetSizeBytes: %d", messageSetSizeBytes)
			offset += 4
		}

		offset, length, err = encodeMessageSet(payload, length, offset, partition, messageSetSizeBytes, buffers, messages)
		if err != nil {
			return offset, length, err
		}
	}
}

func encodePartitionResponses(payload []byte, length int, offset int, buffers chan []byte, messages chan *Message) (int, int, error) {
	var (
		partitionResponseCount int = -1
		counter                int = 0

		buffer []byte
		more   bool
		err    error
	)

	for {
		buffer, more = <-buffers
		if more {
			copy(payload[length:], buffer)
			length += len(buffer)
			glog.V(10).Infof("%d bytes in fetch response payload", length)
		}

		if partitionResponseCount == -1 {
			if offset+4 > length {
				continue
			}
			partitionResponseCount = int(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4

			if partitionResponseCount == 0 {
				return offset, length, nil
			}
		}

		offset, length, err = encodePartitionResponse(payload, length, offset, buffers, messages)
		if err != nil {
			return offset, length, err
		}
		counter++

		if counter == partitionResponseCount {
			return offset, length, nil
		}
	}
}

func encodeResponses(payload []byte, length int, buffers chan []byte, messages chan *Message) error {
	var (
		topicName       string = ""
		topicNameLength int    = -1
		offset          int    = 12 // processed offset
		more            bool   = true
		buffer          []byte
		err             error
	)

	for {
		if !more {
			//if topicNameLength == -1 || topicName == "" {
			//glog.Error("could not read more data from fetch response, but not get topicname yet")
			//}
			return &notEnoughDataInFetchResponse
		}

		buffer, more = <-buffers
		if more {
			copy(payload[length:], buffer)
			length += len(buffer)
			glog.V(10).Infof("%d bytes in fetch response payload", length)
		}

		if topicNameLength == -1 {
			if offset+2 > length {
				continue
			}
			topicNameLength = int(binary.BigEndian.Uint16(payload[offset:]))
			glog.V(10).Infof("topicNameLength: %d", topicNameLength)
			offset += 2
		}
		if topicName == "" {
			if offset+topicNameLength > length {
				continue
			}
			topicName = string(payload[offset : offset+topicNameLength])
			glog.V(15).Infof("topicName: %s", topicName)
			offset += topicNameLength
		}

		glog.V(10).Infof("more: %v, topicNameLength: %d, offset: %d, length: %d", more, topicNameLength, offset, length)
		glog.V(15).Infof("toppic name: %s", topicName)

		offset, length, err = encodePartitionResponses(payload, length, offset, buffers, messages)
		if err != nil {
			return err
		}
		if !more {
			return nil
		}

		topicName = ""
		topicNameLength = -1
	}
}

func consumeFetchResponse(buffers chan []byte, messages chan *Message) {
	defer func() {
		glog.V(10).Info("consumeFetchResponse return")
		close(messages)
	}()

	var payload []byte
	length := 0
	payloadLengthBuf := make([]byte, 0)
	for {
		buffer := <-buffers
		glog.V(20).Infof("%v", buffer)
		length += len(buffer)
		glog.V(10).Infof("%d bytes in fetch response payload", length)
		payloadLengthBuf := append(payloadLengthBuf, buffer...)
		if len(payloadLengthBuf) >= 4 {
			responseLength := binary.BigEndian.Uint32(payloadLengthBuf)
			glog.V(10).Infof("responseLength: %d", responseLength)
			payload = make([]byte, responseLength+4)
			copy(payload, payloadLengthBuf)
			break
		}
	}

	// header
	for {
		buffer, more := <-buffers
		if more {
			glog.V(20).Infof("%v", buffer)
			copy(payload[length:], buffer)
			length += len(buffer)
			glog.V(10).Infof("%d bytes in fetch response payload", length)
			if length >= 12 {
				break
			}
		} else {
			glog.Fatal("NOT get enough data to build FetchResponse")
			return
		}
	}

	glog.V(20).Infof("%d: %v", length, payload[:length])
	correlationID := binary.BigEndian.Uint32(payload[4:])
	glog.V(10).Infof("correlationID: %d", correlationID)

	responsesCount := binary.BigEndian.Uint32(payload[8:])
	glog.V(10).Infof("responsesCount: %d", responsesCount)
	if responsesCount == 0 {
		return
	}
	err := encodeResponses(payload, length, buffers, messages)
	if err != nil && err != &fetchResponseTruncated {
		glog.Fatal(err)
	}
}
