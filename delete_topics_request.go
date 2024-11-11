package healer

import (
	"encoding/binary"
)

// DeleteTopicsRequest Request holds the argument of DeleteTopicsRequest
type DeleteTopicsRequest struct {
	*RequestHeader
	TopicsNames []string `json:"topics_names"`
	TimeoutMS   int32    `json:"timeout_ms"`
}

// NewDeleteTopicsRequest creates a new DeleteTopicsRequest
func NewDeleteTopicsRequest(clientID string, topicsNames []string, timeoutMS int32) DeleteTopicsRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_DeleteTopics,
		APIVersion: 0,
		ClientID:   &clientID,
	}
	return DeleteTopicsRequest{
		RequestHeader: requestHeader,
		TopicsNames:   topicsNames,
		TimeoutMS:     timeoutMS,
	}
}

func (r DeleteTopicsRequest) length() int {
	requestLength := r.RequestHeader.length() + 4
	for _, topicName := range r.TopicsNames {
		requestLength += 2 + len(topicName)
	}
	requestLength += 4
	return requestLength
}

// Encode encodes DeleteTopicsRequest to []byte
func (r DeleteTopicsRequest) Encode(version uint16) []byte {
	requestLength := r.length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += r.RequestHeader.EncodeTo(payload[offset:])

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.TopicsNames)))
	offset += 4

	for _, topicName := range r.TopicsNames {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(topicName)))
		offset += 2
		offset += copy(payload[offset:], topicName)
	}

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.TimeoutMS))

	return payload
}
