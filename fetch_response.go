package healer

import (
	"encoding/binary"
	"errors"

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
	offset := uint64(0)

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
		topicNameLength := uint64(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		fetchResponse.Responses[i].TopicName = string(payload[offset : offset+topicNameLength])
		offset += topicNameLength
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
			//return
			fetchResponse.Responses[i].PartitionResponses[j].MessageSet = make([]*Message, 0)
			for {
				if payloadLength == offset {
					break
				}
				//if payloadLength< offset+26 {
				if payloadLength < offset+26 {
					glog.V(5).Infof("response is truncated because of max-bytes parameter in fetch request(resopnseLength[%d]+4 < offset[%d]+26).", responseLength, offset)
					if len(fetchResponse.Responses[i].PartitionResponses[j].MessageSet) == 0 {
						return errors.New("MaxBytes parameter is to small for server to send back one whole message.")
					}
					return nil
				}
				glog.V(10).Infof("offset: %d", offset)
				glog.V(20).Infof("playload %v", payload[offset:])
				message := &Message{}
				message.Offset = int64(binary.BigEndian.Uint64(payload[offset:]))
				offset += 8
				glog.V(10).Infof("message offset: %d", message.Offset)

				message.MessageSize = int32(binary.BigEndian.Uint32(payload[offset:]))
				glog.V(10).Infof("message size: %d", message.MessageSize)
				offset += 4

				if payloadLength < offset+uint64(message.MessageSize) {
					glog.V(5).Infof("response is truncated because of max-bytes parameter in fetch request(payloadLength[%d] < offset[%d]+messageSize[%d]).", payloadLength, offset, message.MessageSize)
					if len(fetchResponse.Responses[i].PartitionResponses[j].MessageSet) == 0 {
						return errors.New("MaxBytes parameter is to small for server to send back one whole message.")
					}
					return nil
				}

				message.Crc = binary.BigEndian.Uint32(payload[offset:])
				offset += 4
				message.MagicByte = int8(payload[offset])
				offset++
				message.Attributes = int8(payload[offset])
				offset++
				keyLength := int32(binary.BigEndian.Uint32(payload[offset:]))
				offset += 4
				if keyLength == -1 {
					message.Key = nil
				} else {
					message.Key = make([]byte, keyLength)
					copy(message.Key, payload[offset:offset+uint64(keyLength)])
					offset += uint64(keyLength)
				}

				valueLength := int32(binary.BigEndian.Uint32(payload[offset:]))
				offset += 4
				if valueLength == -1 {
					message.Value = nil
				} else {
					message.Value = make([]byte, valueLength)
					copy(message.Value, payload[offset:offset+uint64(valueLength)])
					glog.V(20).Infof("message value: %s", message.Value)
					offset += uint64(valueLength)
				}
				fetchResponse.Responses[i].PartitionResponses[j].MessageSet = append(fetchResponse.Responses[i].PartitionResponses[j].MessageSet, message)
			}
		}
	}
	return nil
}
