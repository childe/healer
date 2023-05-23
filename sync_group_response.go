package healer

import (
	"encoding/binary"
	"fmt"
)

type SyncGroupResponse struct {
	CorrelationID    uint32
	ErrorCode        int16
	MemberAssignment []byte
}

func (r SyncGroupResponse) Error() error {
	return getErrorFromErrorCode(r.ErrorCode)
}

func NewSyncGroupResponse(payload []byte) (*SyncGroupResponse, error) {
	var err error = nil
	r := &SyncGroupResponse{}
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("syncgroup reseponse length did not match: %d!=%d", responseLength+4, len(payload))
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
