package healer

import (
	"encoding/binary"
	"fmt"
)

/* =========
Produce Response (Version: 0) => [responses]
  responses => topic [partition_responses]
    topic => STRING
    partition_responses => partition error_code base_offset
      partition => INT32
      error_code => INT16
      base_offset => INT64

=========

FIELD	DESCRIPTION
responses	null
topic	Name of topic
partition_responses	null
partition	Topic partition id
error_code	Response error code
base_offset	null
 ========= */

type ProduceResponse_PartitionResponse struct {
	PartitionID int32
	ErrorCode   int16
	BaseOffset  int64
}

type ProduceResponsePiece struct {
	Topic      string
	Partitions []*ProduceResponse_PartitionResponse
}

type ProduceResponse struct {
	CorrelationID    uint32
	ProduceResponses []*ProduceResponsePiece
}

func NewProduceResponse(payload []byte) (*ProduceResponse, error) {
	var (
		r      *ProduceResponse = &ProduceResponse{}
		err    error            = nil
		offset int              = 0
		l      int              = 0
	)
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("produce reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	produceResponsePieceCount := binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.ProduceResponses = make([]*ProduceResponsePiece, produceResponsePieceCount)

	for _, produceResponse := range r.ProduceResponses {
		produceResponse = &ProduceResponsePiece{}
		l = int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		produceResponse.Topic = string(payload[offset : offset+l])
		offset += l

		ProduceResponse_PartitionResponse_Count := binary.BigEndian.Uint32(payload[offset:])
		offset += 4
		produceResponse.Partitions = make([]*ProduceResponse_PartitionResponse, ProduceResponse_PartitionResponse_Count)
		for _, p := range produceResponse.Partitions {
			p = &ProduceResponse_PartitionResponse{}
			p.PartitionID = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			p.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2
			if err == nil && p.ErrorCode != 0 {
				err = getErrorFromErrorCode(p.ErrorCode)
			}
			p.BaseOffset = int64(binary.BigEndian.Uint64(payload[offset:]))
			offset += 8
		}
	}

	return r, err
}
