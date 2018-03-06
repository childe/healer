package healer

import (
	"encoding/binary"
)

/*
OffsetsResponse => [TopicName [PartitionOffsets]]
  PartitionOffsets => Partition ErrorCode [Offset]
  Partition => int32
  ErrorCode => int16
  Offset => int64
*/

// TODO rename
type PartitionOffset struct {
	Partition int32
	ErrorCode int16
	Offsets   []uint64
}
type OffsetsResponse struct {
	CorrelationID         uint32
	TopicPartitionOffsets map[string][]*PartitionOffset
}

func NewOffsetsResponse(payload []byte) (*OffsetsResponse, error) {
	offsetsResponse := &OffsetsResponse{}
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		//TODO lenght does not match
	}
	offset += 4

	offsetsResponse.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	topicLenght := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	offsetsResponse.TopicPartitionOffsets = make(map[string][]*PartitionOffset)
	for i := 0; i < topicLenght; i++ {
		topicNameLenght := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		topicName := string(payload[offset : offset+topicNameLenght])
		offset += topicNameLenght

		partitionOffsetLength := binary.BigEndian.Uint32(payload[offset:])
		offset += 4
		offsetsResponse.TopicPartitionOffsets[topicName] = make([]*PartitionOffset, partitionOffsetLength)
		for j := uint32(0); j < partitionOffsetLength; j++ {
			partition := int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			errorCode := int16(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2
			offsetLength := binary.BigEndian.Uint32(payload[offset:])
			offset += 4
			offsetsResponse.TopicPartitionOffsets[topicName][j] = &PartitionOffset{
				Partition: partition,
				ErrorCode: errorCode,
				Offsets:   make([]uint64, offsetLength),
			}
			for k := uint32(0); k < offsetLength; k++ {
				offsetsResponse.TopicPartitionOffsets[topicName][j].Offsets[k] = binary.BigEndian.Uint64(payload[offset:])
				offset += 8
			}
		}
	}
	return offsetsResponse, nil
}
