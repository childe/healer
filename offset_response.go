package healer

import (
	"encoding/binary"
	"fmt"
)

type PartitionOffset struct {
	Partition       int32
	ErrorCode       int16
	OldStyleOffsets []int64
	Timestamp       int64
	Offset          int64
}
type OffsetsResponse struct {
	CorrelationID         uint32
	ThrottleTimeMs        int32
	TopicPartitionOffsets map[string][]PartitionOffset
}

func (r OffsetsResponse) Error() error {
	for topic, partitionOffsets := range r.TopicPartitionOffsets {
		for _, offset := range partitionOffsets {
			if offset.ErrorCode != 0 {
				return fmt.Errorf("offsets response error of %s-%d: %w", topic, offset.Partition, KafkaError(offset.ErrorCode))
			}
		}
	}
	return nil
}

func NewOffsetsResponse(payload []byte, version uint16) (r OffsetsResponse, err error) {
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("offsets response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	if version > 0 {
		r.ThrottleTimeMs = int32(binary.BigEndian.Uint32(payload[offset:]))
	}

	topicLenght := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	r.TopicPartitionOffsets = make(map[string][]PartitionOffset)
	for i := 0; i < topicLenght; i++ {
		topicNameLenght := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		topicName := string(payload[offset : offset+topicNameLenght])
		offset += topicNameLenght

		partitionOffsetLength := binary.BigEndian.Uint32(payload[offset:])
		offset += 4
		r.TopicPartitionOffsets[topicName] = make([]PartitionOffset, partitionOffsetLength)
		for j := uint32(0); j < partitionOffsetLength; j++ {
			p := PartitionOffset{}

			p.Partition = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			p.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2

			if version == 0 {
				offsetLength := binary.BigEndian.Uint32(payload[offset:])
				offset += 4

				for k := uint32(0); k < offsetLength; k++ {
					p.OldStyleOffsets = append(p.OldStyleOffsets, int64(binary.BigEndian.Uint64(payload[offset:])))
					offset += 8
				}
			} else {
				p.Timestamp = int64(binary.BigEndian.Uint64(payload[offset:]))
				offset += 8
				p.Offset = int64(binary.BigEndian.Uint64(payload[offset:]))
				offset += 8
			}
			r.TopicPartitionOffsets[topicName][j] = p
		}
	}
	return r, nil
}
