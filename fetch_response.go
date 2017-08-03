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
  MessageSetSize => int32

Field					Description
HighwaterMarkOffset		The offset at the end of the log for this partition. This can be used by the client to determine how many messages behind the end of the log they are.
MessageSet				The message data fetched from this partition, in the format described above.
MessageSetSize			The size in bytes of the message set for this partition
Partition				The id of the partition this response is for.
TopicName				The name of the topic this response entry is for.
*/

// PartitionResponse stores partitionID and MessageSet in the partition
type PartitionResponse struct {
	Partition           int32
	ErrorCode           int16
	HighwaterMarkOffset int64
	MessageSetSize      int32
	MessageSet          MessageSet
}

// FetchResponse stores topicname and arrya of PartitionResponse
type FetchResponse struct {
	CorrelationId int32
	Responses     []struct {
		TopicName  string
		PartitionResponses []PartitionResponse
	}
}

// Decode payload stored in byte array to FetchResponse object
func (fetchResponse *FetchResponse) Decode(payload []byte) {
	offset := uint64(0)

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		//TODO lenght does not match
	}
	offset += 4

	fetchResponse.CorrelationId = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	responsesCount := binary.BigEndian.Uint32(payload[offset:])
	offset += 4
	glog.V(10).Infof("there is %d topic in fetch response", responsesCount)

	fetchResponse.Responses = make([]struct {
		TopicName  string
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
			offset += 4
			fetchResponse.Responses[i].PartitionResponses[j].ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2
			fetchResponse.Responses[i].PartitionResponses[j].HighwaterMarkOffset = int64(binary.BigEndian.Uint64(payload[offset:]))
			offset += 8
			fetchResponse.Responses[i].PartitionResponses[j].MessageSetSize = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			fetchResponse.Responses[i].PartitionResponses[j].MessageSet = make([]Message, fetchResponse.Responses[i].PartitionResponses[j].MessageSetSize/26)
			for k := int32(0); k < fetchResponse.Responses[i].PartitionResponses[j].MessageSetSize; k++ {
				fetchResponse.Responses[i].PartitionResponses[j].MessageSet[k].Offset = int64(binary.BigEndian.Uint64(payload[offset:]))
				offset += 8
				fetchResponse.Responses[i].PartitionResponses[j].MessageSet[k].MessageSize = int32(binary.BigEndian.Uint32(payload[offset:]))
				offset += 4
				fetchResponse.Responses[i].PartitionResponses[j].MessageSet[k].Crc = binary.BigEndian.Uint32(payload[offset:])
				offset += 4
				fetchResponse.Responses[i].PartitionResponses[j].MessageSet[k].MagicByte = int8(payload[offset])
				offset++
				fetchResponse.Responses[i].PartitionResponses[j].MessageSet[k].Attributes = int8(payload[offset])
				offset++
				keyLength := int32(binary.BigEndian.Uint32(payload[offset:]))
				offset += 4
				if keyLength == -1 {
					fetchResponse.Responses[i].PartitionResponses[j].MessageSet[k].Key = nil
				} else {
					fetchResponse.Responses[i].PartitionResponses[j].MessageSet[k].Key = make([]byte, keyLength)
					copy(fetchResponse.Responses[i].PartitionResponses[j].MessageSet[k].Key, payload[offset:offset+uint64(keyLength)])
					offset += uint64(keyLength)
				}

				valueLength := int32(binary.BigEndian.Uint32(payload[offset:]))
				offset += 4
				if valueLength == -1 {
					fetchResponse.Responses[i].PartitionResponses[j].MessageSet[k].Value = nil
				} else {
					fetchResponse.Responses[i].PartitionResponses[j].MessageSet[k].Value = make([]byte, valueLength)
					copy(fetchResponse.Responses[i].PartitionResponses[j].MessageSet[k].Value, payload[offset:offset+uint64(valueLength)])
					offset += uint64(valueLength)
				}
				if offset == uint64(len(payload)) {
					fetchResponse.Responses[i].PartitionResponses[j].MessageSet = fetchResponse.Responses[i].PartitionResponses[j].MessageSet[:k+1]
					break
				}
			}
		}
	}
}
