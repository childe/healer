package healer

import (
	"encoding/binary"
	"fmt"
)

// IncrementalAlterConfigsResponse struct holds params in AlterConfigsRequest
type IncrementalAlterConfigsResponse struct {
	CorrelationID  uint32                                    `json:"correlation_id"`
	ThrottleTimeMs uint32                                    `json:"throttle_time_ms"`
	Resources      []IncrementalAlterConfigsResponseResource `json:"resources"`
}

func (r IncrementalAlterConfigsResponse) Error() error {
	for _, e := range r.Resources {
		if e.ErrorCode != 0 {
			return KafkaError(e.ErrorCode)
		}
	}
	return nil
}

// IncrementalAlterConfigsResponseResource is sub struct in AlterConfigsRequest
type IncrementalAlterConfigsResponseResource struct {
	ErrorCode    int16  `json:"error_code"`
	ErrorMessage string `json:"error_message"`
	ResourceType uint8  `json:"resource_type"`
	ResourceName string `json:"resource_name"`
}

func decodeToIncrementalAlterConfigsResponseResource(payload []byte, version uint16) (r IncrementalAlterConfigsResponseResource, resourceLength int) {
	offset := 0
	r.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	l := int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if l > 0 {
		r.ErrorMessage = string(payload[offset : offset+int(l)])
		offset += int(l)
	}

	r.ResourceType = payload[offset]
	offset++

	l = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if l > 0 {
		r.ResourceName = string(payload[offset : offset+int(l)])
		offset += int(l)
	}
	return
}

// NewIncrementalAlterConfigsResponse create a new IncrementalAlterConfigsResponse.
// This does not return error in the response. user may need to check the error code in the response by themselves.
func NewIncrementalAlterConfigsResponse(payload []byte, version uint16) (r IncrementalAlterConfigsResponse, err error) {
	var (
		offset int = 0
	)
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("alterconfig response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.ThrottleTimeMs = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	count := binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.Resources = make([]IncrementalAlterConfigsResponseResource, count)
	for i := range r.Resources {
		e, o := decodeToIncrementalAlterConfigsResponseResource(payload[offset:], version)
		offset += o
		r.Resources[i] = e
	}

	return r, err
}
