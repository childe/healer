package healer

import (
	"encoding/binary"
	"fmt"
)

// SyncGroupResponse is the response of syncgroup request
type SyncGroupResponse struct {
	CorrelationID    uint32 `json:"correlation_id"`
	ErrorCode        int16  `json:"error_code"`
	MemberAssignment []byte `json:"member_assignment"`
}

func (r SyncGroupResponse) Error() error {
	return getErrorFromErrorCode(r.ErrorCode)
}

// NewSyncGroupResponse create a NewSyncGroupResponse instance from response payload bytes
func NewSyncGroupResponse(payload []byte) (r SyncGroupResponse, err error) {
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("syncgroup response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if err == nil && r.ErrorCode != 0 {
		err = getErrorFromErrorCode(r.ErrorCode)
	}

	memberAssignmentLength := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	r.MemberAssignment = make([]byte, memberAssignmentLength)
	copy(r.MemberAssignment, payload[offset:offset+memberAssignmentLength])
	offset += memberAssignmentLength

	return r, err
}
