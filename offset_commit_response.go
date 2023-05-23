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
	ErrorCode   int16
}

type OffsetCommitResponseTopic struct {
	Topic      string
	Partitions []*OffsetCommitResponsePartition
}

type OffsetCommitResponse struct {
	CorrelationID uint32
	Topics        []*OffsetCommitResponseTopic
}

func (r OffsetCommitResponse) Error() error {
	for _, topic := range r.Topics {
		for _, partition := range topic.Partitions {
			if partition.ErrorCode != 0 {
				return getErrorFromErrorCode(partition.ErrorCode)
			}
		}
	}
	return nil
}

func NewOffsetCommitResponse(payload []byte) (r OffsetCommitResponse, err error) {
	var (
		offset int = 0
		l      int = 0
	)
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("offsetcommit reseponse length did not match: %d!=%d", responseLength+4, len(payload))
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
			p.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2

			if err == nil && p.ErrorCode != 0 {
				err = getErrorFromErrorCode(p.ErrorCode)
			}
		}
	}

	return r, err
}
