package healer

import (
	"encoding/binary"
	"fmt"
)

/*
OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
  TopicName => string
  Partition => int32
  ErrorCode => int16
*/

type OffsetCommitResponsePartition struct {
	PartitionID uint32
	ErrorCode   uint16
}

type OffsetCommitResponseTopic struct {
	Topic      string
	Partitions []*OffsetCommitResponsePartition
}

type OffsetCommitResponse struct {
	CorrelationID uint32
	Topics        []*OffsetCommitResponseTopic
}

func NewOffsetCommitResponse(payload []byte) (*OffsetCommitResponse, error) {
	var (
		r      *OffsetCommitResponse = &OffsetCommitResponse{}
		err    error                 = nil
		offset int                   = 0
		l      int                   = 0
	)
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("offsetcommit reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	l = int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	r.Topics = make([]*OffsetCommitResponseTopic, l)
	for i := range r.Topics {
		topic := &OffsetCommitResponseTopic{}
		r.Topics[i] = topic

		l = int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		topic.Topic = string(payload[offset : offset+l])
		offset += l

		l = int(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		topic.Partitions = make([]*OffsetCommitResponsePartition, l)
		for j := range topic.Partitions {
			p := &OffsetCommitResponsePartition{}
			topic.Partitions[j] = p

			p.PartitionID = binary.BigEndian.Uint32(payload[offset:])
			offset += 4
			p.ErrorCode = binary.BigEndian.Uint16(payload[offset:])
			offset += 2

			if err == nil && p.ErrorCode != 0 {
				err = AllError[p.ErrorCode]
			}
		}
	}

	return r, err
}
