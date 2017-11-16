package healer

import (
	"encoding/binary"
	"fmt"

	"github.com/golang/glog"
)

//SyncGroup Response (Version: 0) => error_code member_assignment
//error_code => INT16
//member_assignment => BYTES

//FIELD	DESCRIPTION
//error_code	Response error code
//member_assignment	null

// version 0
type SyncGroupResponse struct {
	CorrelationID    uint32
	ErrorCode        uint16
	MemberAssignment []byte
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

	r.ErrorCode = binary.BigEndian.Uint16(payload[offset:])
	glog.Info(r.ErrorCode)
	offset += 2
	if r.ErrorCode != 0 {
		err = AllError[r.ErrorCode]
	}

	memberAssignmentLength := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	copy(r.MemberAssignment, payload[offset:offset+memberAssignmentLength])
	offset += memberAssignmentLength

	return r, err
}
