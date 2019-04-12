package healer

import (
	"encoding/binary"
)

/*
Heartbeat Request (Version: 0) => group_id generation_id member_id
  group_id => STRING
  generation_id => INT32
  member_id => STRING

FIELD	DESCRIPTION
group_id	The unique group identifier
generation_id	The generation of the group.
member_id	The member id assigned by the group coordinator or null if joining for the first time.
*/

// TODO version0
type HeartbeatRequest struct {
	RequestHeader *RequestHeader
	GroupID       string
	GenerationID  int32
	MemberID      string
}

func NewHeartbeatRequest(clientID, groupID string, generationID int32, memberID string) *HeartbeatRequest {
	requestHeader := &RequestHeader{
		ApiKey:     API_Heartbeat,
		ApiVersion: 0,
		ClientId:   clientID,
	}
	return &HeartbeatRequest{
		RequestHeader: requestHeader,
		GroupID:       groupID,
		GenerationID:  generationID,
		MemberID:      memberID,
	}
}

func (heartbeatR *HeartbeatRequest) Length() int {
	requestLength := heartbeatR.RequestHeader.length() + 2 + len(heartbeatR.GroupID) + 4 + 2 + len(heartbeatR.MemberID)
	return requestLength
}

func (heartbeatR *HeartbeatRequest) Encode() []byte {
	requestLength := heartbeatR.Length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = heartbeatR.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(heartbeatR.GroupID)))
	offset += 2
	offset += copy(payload[offset:], heartbeatR.GroupID)

	binary.BigEndian.PutUint32(payload[offset:], uint32(heartbeatR.GenerationID))
	offset += 4

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(heartbeatR.MemberID)))
	offset += 2
	copy(payload[offset:], heartbeatR.MemberID)

	return payload
}

func (req *HeartbeatRequest) API() uint16 {
	return req.RequestHeader.ApiKey
}

func (req *HeartbeatRequest) SetCorrelationID(c uint32) {
	req.RequestHeader.CorrelationID = c
}
