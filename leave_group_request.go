package healer

import (
	"encoding/binary"
)

//LeaveGroup Request (Version: 0) => group_id member_id
//group_id => STRING
//member_id => STRING

//FIELD	DESCRIPTION
//group_id	The unique group identifier
//member_id	The member id assigned by the group coordinator or null if leaveing for the first time.

// version 0
type LeaveGroupRequest struct {
	*RequestHeader
	GroupID  string
	MemberID string
}

func NewLeaveGroupRequest(clientID, groupID, memberID string) *LeaveGroupRequest {
	requestHeader := &RequestHeader{
		ApiKey:     API_LeaveGroup,
		ApiVersion: 0,
		ClientId:   clientID,
	}

	return &LeaveGroupRequest{
		RequestHeader: requestHeader,
		GroupID:       groupID,
		MemberID:      memberID,
	}
}

func (r *LeaveGroupRequest) Length() int {
	l := r.RequestHeader.length() + 2 + len(r.GroupID) + 2 + len(r.MemberID)
	return l
}

func (r *LeaveGroupRequest) Encode() []byte {
	requestLength := r.Length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = r.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.GroupID)))
	offset += 2
	offset += copy(payload[offset:], r.GroupID)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.MemberID)))
	offset += 2
	offset += copy(payload[offset:], r.MemberID)

	return payload
}
