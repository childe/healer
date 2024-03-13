package healer

import (
	"encoding/binary"
	"fmt"
)

// CreateTopicsResponse is response of create_topics request
type CreateTopicsResponse struct {
	CorrelationID uint32
	TopicErrors   []TopicError
}

// TopicError is sub struct in CreateTopicsResponse
type TopicError struct {
	Topic     string
	ErrorCode int16
}

func (r CreateTopicsResponse) Error() error {
	for _, e := range r.TopicErrors {
		if e.ErrorCode != 0 {
			return KafkaError(e.ErrorCode)
		}
	}
	return nil
}

// NewCreateTopicsResponse decode binary bytes to CreateTopicsResponse struct
func NewCreateTopicsResponse(payload []byte) (r CreateTopicsResponse, err error) {
	var (
		offset int
	)

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("create_topics response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	count := int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.TopicErrors = make([]TopicError, count)
	for i := range r.TopicErrors {
		e := TopicError{}

		l := int(int16(binary.BigEndian.Uint16(payload[offset:])))
		offset += 2

		e.Topic = string(payload[offset : offset+l])
		offset += l

		e.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		r.TopicErrors[i] = e
	}

	return r, err
}
