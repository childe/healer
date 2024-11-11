package healer

import (
	"encoding/binary"
)

type ProduceRequest struct {
	*RequestHeader
	RequiredAcks int16
	Timeout      int32
	TopicBlocks  []struct {
		TopicName      string
		PartitonBlocks []struct {
			Partition      int32
			MessageSetSize int32
			MessageSet     MessageSet
		}
	}
}

func (produceRequest *ProduceRequest) Length() int {
	requestLength := produceRequest.RequestHeader.length() + 10 //	RequiredAcks(2) + Timeout(4) + TopicBlocks_length(4)
	for _, topicBlock := range produceRequest.TopicBlocks {
		requestLength += 6 + len(topicBlock.TopicName)
		for _, parttionBlock := range topicBlock.PartitonBlocks {
			requestLength += 8 + parttionBlock.MessageSet.Length()
		}
	}

	return requestLength
}

func (produceRequest *ProduceRequest) Encode(version uint16) []byte {
	requestLength := produceRequest.Length()
	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += produceRequest.RequestHeader.EncodeTo(payload[offset:])

	binary.BigEndian.PutUint16(payload[offset:], uint16(produceRequest.RequiredAcks))
	offset += 2
	binary.BigEndian.PutUint32(payload[offset:], uint32(produceRequest.Timeout))
	offset += 4

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(produceRequest.TopicBlocks)))
	offset += 4
	for _, topicBlock := range produceRequest.TopicBlocks {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(topicBlock.TopicName)))
		offset += 2
		offset += copy(payload[offset:], topicBlock.TopicName)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(topicBlock.PartitonBlocks)))
		offset += 4
		for _, parttionBlock := range topicBlock.PartitonBlocks {
			binary.BigEndian.PutUint32(payload[offset:], uint32(parttionBlock.Partition))
			offset += 4
			binary.BigEndian.PutUint32(payload[offset:], uint32(parttionBlock.MessageSet.Length()))
			offset += 4

			offset = parttionBlock.MessageSet.Encode(payload, offset)
		}
	}

	return payload
}
