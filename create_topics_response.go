package healer

import (
	"encoding/binary"
	"fmt"
)

// CreateTopicsResponse is described in http://kafka.apache.org/protocol.html
// CreateTopics Response (Version: 0) => [topic_errors]
//   topic_errors => topic error_code
//     topic => STRING
//     error_code => INT16
type CreateTopicsResponse struct {
	CorrelationID uint32
	TopicErrors   []*TopicError
}

// TopicErrors is sub struct in CreateTopicsResponse
type TopicError struct {
	Topic     string
	ErrorCode int16
}

// NewCreateTopicsResponse decode binary bytes to CreateTopicsResponse struct
func NewCreateTopicsResponse(payload []byte) (*CreateTopicsResponse, error) {
	var (
		err    error
		offset int
		r      = &CreateTopicsResponse{}
	)

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("create_topics reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	count := int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.TopicErrors = make([]*TopicError, count)
	for i := range r.TopicErrors {
		e := &TopicError{}
		r.TopicErrors[i] = e

		l := int(int16(binary.BigEndian.Uint16(payload[offset:])))
		offset += 2

		e.Topic = string(payload[offset : offset+l])
		offset += l

		e.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
		if err == nil && e.ErrorCode != 0 {
			err = getErrorFromErrorCode(e.ErrorCode)
		}
	}

	return r, err
}
