package healer

import (
	"encoding/binary"
	"fmt"
)

/*
OffsetFetch Response (Version: 0) => [responses]
  responses => topic [partition_responses]
    topic => STRING
    partition_responses => partition offset metadata error_code
      partition => INT32
      offset => INT64
      metadata => NULLABLE_STRING
      error_code => INT16


FIELD	DESCRIPTION
responses	null
topic	Name of topic
partition_responses	null
partition	Topic partition id
offset	Last committed message offset.
metadata	Any associated metadata the client wants to keep.
error_code	Response error code
*/

type OffsetFetchResponsePartition struct {
	PartitionID uint32
	Offset      int64
	Metadata    string
	ErrorCode   int16
}

type OffsetFetchResponseTopic struct {
	Topic      string
	Partitions []*OffsetFetchResponsePartition
}

type OffsetFetchResponse struct {
	CorrelationID uint32
	Topics        []*OffsetFetchResponseTopic
}

func NewOffsetFetchResponse(payload []byte) (*OffsetFetchResponse, error) {
	var (
		r      *OffsetFetchResponse = &OffsetFetchResponse{}
		err    error                = nil
		offset int                  = 0
		l      int                  = 0
	)
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("offsetfetch reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	l = int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	r.Topics = make([]*OffsetFetchResponseTopic, l)
	for i := range r.Topics {
		topic := &OffsetFetchResponseTopic{}
		r.Topics[i] = topic

		l = int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		topic.Topic = string(payload[offset : offset+l])
		offset += l

		l = int(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		topic.Partitions = make([]*OffsetFetchResponsePartition, l)
		for j := range topic.Partitions {
			p := &OffsetFetchResponsePartition{}
			topic.Partitions[j] = p

			p.PartitionID = binary.BigEndian.Uint32(payload[offset:])
			offset += 4
			p.Offset = int64(binary.BigEndian.Uint64(payload[offset:]))
			offset += 8
			l = int(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2
			p.Metadata = string(payload[offset : offset+l])
			offset += l
			p.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2

			if err == nil && p.ErrorCode != 0 {
				err = getErrorFromErrorCode(p.ErrorCode)
			}
		}
	}

	return r, err
}
