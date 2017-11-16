package healer

import (
	"encoding/binary"
	"fmt"
)

// version 0
type LeaveGroupResponse struct {
	CorrelationID uint32
	ErrorCode     uint16
}

func NewLeaveGroupResponse(payload []byte) (*LeaveGroupResponse, error) {
	var err error = nil
	r := &LeaveGroupResponse{}
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("leaveGroup reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ErrorCode = binary.BigEndian.Uint16(payload[offset:])

	if r.ErrorCode != 0 {
		err = AllError[r.ErrorCode]
	}

	return r, err
}
