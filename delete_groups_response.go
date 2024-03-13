package healer

import (
	"encoding/binary"
	"fmt"
)

// DeleteGroupsResponse Request holds the argument of DeleteGroupsResponse
type DeleteGroupsResponse struct {
	CorrelationID  uint32 `json:"correlation_id"`
	ThrottleTimeMs int32  `json:"throttle_time_ms"`
	Results        []struct {
		GroupID   string `json:"group_id"`
		ErrorCode int16  `json:"error_code"`
	} `json:"results"`
}

// FIXME: multiple error code
func (r DeleteGroupsResponse) Error() error {
	for _, result := range r.Results {
		if result.ErrorCode != 0 {
			return fmt.Errorf("delete group[%s] failed: %s", result.GroupID, KafkaError(result.ErrorCode))
		}
	}
	return nil
}

// NewDeleteGroupsResponse creates a new DeleteGroupsResponse from []byte
func NewDeleteGroupsResponse(payload []byte) (r DeleteGroupsResponse, err error) {
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("delete_groups response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ThrottleTimeMs = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	count := (binary.BigEndian.Uint32(payload[offset:]))
	r.Results = make([]struct {
		GroupID   string `json:"group_id"`
		ErrorCode int16  `json:"error_code"`
	}, count)
	offset += 4

	for i := uint32(0); i < count; i++ {
		groupIDLength := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		r.Results[i].GroupID = string(payload[offset : offset+groupIDLength])
		offset += groupIDLength

		r.Results[i].ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
	}

	return
}
