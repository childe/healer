package healer

import (
	"encoding/binary"
	"fmt"
)

type ProduceResponse_PartitionResponse struct {
	PartitionID int32
	ErrorCode   int16
	BaseOffset  int64
}

type ProduceResponsePiece struct {
	Topic      string
	Partitions []ProduceResponse_PartitionResponse
}

type ProduceResponse struct {
	CorrelationID    uint32
	ProduceResponses []ProduceResponsePiece
}

func (r ProduceResponse) Error() error {
	for _, produceResponse := range r.ProduceResponses {
		for _, partition := range produceResponse.Partitions {
			if partition.ErrorCode != 0 {
				return KafkaError(partition.ErrorCode)
			}
		}
	}
	return nil
}

func NewProduceResponse(payload []byte) (r ProduceResponse, err error) {
	var (
		offset int = 0
		l      int = 0
	)
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("produce response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	produceResponsePieceCount := binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.ProduceResponses = make([]ProduceResponsePiece, produceResponsePieceCount)

	for i := range r.ProduceResponses {
		produceResponse := &r.ProduceResponses[i]
		l = int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		produceResponse.Topic = string(payload[offset : offset+l])
		offset += l

		count := binary.BigEndian.Uint32(payload[offset:])
		offset += 4
		produceResponse.Partitions = make([]ProduceResponse_PartitionResponse, count)
		for j := range produceResponse.Partitions {
			p := &produceResponse.Partitions[j]
			p.PartitionID = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			p.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2
			if err == nil && p.ErrorCode != 0 {
				err = KafkaError(p.ErrorCode)
			}
			p.BaseOffset = int64(binary.BigEndian.Uint64(payload[offset:]))
			offset += 8
		}
	}

	return r, err
}
