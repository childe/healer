package healer

import (
	"encoding/binary"
)

/*
OffsetResponse => [TopicName [PartitionOffsets]]
  PartitionOffsets => Partition ErrorCode [Offset]
  Partition => int32
  ErrorCode => int16
  Offset => int64
*/

type PartitionOffset struct {
	Partition uint32
	ErrorCode int16
	Offset    []uint64
}
type OffsetResponse struct {
	CorrelationId uint32
	Info          map[string][]*PartitionOffset
}

func (offsetResponse *OffsetResponse) Decode(payload []byte) {
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		//TODO lenght does not match
	}
	offset += 4

	offsetResponse.CorrelationId = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	topicLenght := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	offsetResponse.Info = make(map[string][]*PartitionOffset)
	for i := 0; i < topicLenght; i++ {
		topicNameLenght := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		topicName := string(payload[offset : offset+topicNameLenght])
		offset += topicNameLenght

		partitionOffsetLength := binary.BigEndian.Uint32(payload[offset:])
		offset += 4
		offsetResponse.Info[topicName] = make([]*PartitionOffset, partitionOffsetLength)
		for j := uint32(0); j < partitionOffsetLength; j++ {
			partition := binary.BigEndian.Uint32(payload[offset:])
			offset += 4
			errorCode := int16(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2
			offsetLength := binary.BigEndian.Uint32(payload[offset:])
			offset += 4
			offsetResponse.Info[topicName][j] = &PartitionOffset{
				Partition: partition,
				ErrorCode: errorCode,
				Offset:    make([]uint64, offsetLength),
			}
			for k := uint32(0); k < offsetLength; k++ {
				offsetResponse.Info[topicName][j].Offset[k] = binary.BigEndian.Uint64(payload[offset:])
				offset += 8
			}
		}
	}
}
