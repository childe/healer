package healer

import (
	"encoding/binary"
	"fmt"
)

// CreatePartitionsResponse holds the parameters of a create-partitions response
type CreatePartitionsResponse struct {
	CorrelationID  uint32                                `json:"correlation_id"`
	ThrottleTimeMS int32                                 `json:"throttle_time_ms"`
	Results        []createPartitionsResponseResultBlock `json:"results"`
}

type createPartitionsResponseResultBlock struct {
	TopicName    string `json:"topic_name"`
	ErrorCode    int16  `json:"error_code"`
	ErrorMessage string `json:"error_message"`
}

// Error implements the error interface, it returns error from error code in the response
func (r CreatePartitionsResponse) Error() error {
	for _, result := range r.Results {
		if result.ErrorCode != 0 {
			return fmt.Errorf("create partitions error(%d): %s: %w", result.ErrorCode, result.ErrorMessage, getErrorFromErrorCode(result.ErrorCode))
		}
	}
	return nil
}

// NewCreatePartitionsResponse creates a new CreatePartitionsResponse from []byte
func NewCreatePartitionsResponse(payload []byte, version uint16) (r CreatePartitionsResponse, err error) {
	var (
		offset int
	)

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("create_partitions response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.ThrottleTimeMS = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	blockCount := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.Results = make([]createPartitionsResponseResultBlock, blockCount)
	for i := 0; i < blockCount; i++ {
		r.Results[i] = createPartitionsResponseResultBlock{}
		l := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		r.Results[i].TopicName = string(payload[offset : offset+l])
		offset += l

		r.Results[i].ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		l = int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		if l > 0 {
			r.Results[i].ErrorMessage = string(payload[offset : offset+l])
			offset += l
		}
	}

	return
}
