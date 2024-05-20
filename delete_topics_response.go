package healer

import (
	"encoding/binary"
	"fmt"
)

// DeleteTopicsResponse Request holds the argument of DeleteTopicsResponse
type DeleteTopicsResponse struct {
	CorrelationID uint32 `json:"correlation_id"`
	Results       []struct {
		TopicName string `json:"topic_name"`
		ErrorCode int16  `json:"error_code"`
	} `json:"results"`
}

// Error returns error list of all failed topics
func (r DeleteTopicsResponse) Error() error {
	var allErrors []error
	for _, result := range r.Results {
		if result.ErrorCode != 0 {
			allErrors = append(allErrors,
				fmt.Errorf("delete topic[%s] failed: %s",
					result.TopicName, KafkaError(result.ErrorCode)))
		}
	}
	if (len(allErrors)) > 0 {
		return fmt.Errorf("delete topics failed: %v", allErrors)
	}
	return nil
}

// NewDeleteTopicsResponse creates a new DeleteTopicsResponse from []byte
func NewDeleteTopicsResponse(payload []byte, version uint16) (r DeleteTopicsResponse, err error) {
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("delete_topics response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	count := (binary.BigEndian.Uint32(payload[offset:]))
	r.Results = make([]struct {
		TopicName string `json:"topic_name"`
		ErrorCode int16  `json:"error_code"`
	}, count)
	offset += 4

	for i := uint32(0); i < count; i++ {
		topicNameLength := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		r.Results[i].TopicName = string(payload[offset : offset+topicNameLength])
		offset += topicNameLength

		r.Results[i].ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
	}

	return
}
