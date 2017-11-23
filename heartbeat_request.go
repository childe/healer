package healer

import (
	"encoding/binary"
)

// TODO version0
type HeartbeatRequest struct {
	RequestHeader *RequestHeader
	GroupID       string
	GenerationID  int32
	MemberID      string
}

func NewHeartbeatRequest(correlationID uint32, clientID, groupID string, generationID int32, memberID string) *HeartbeatRequest {
	requestHeader := &RequestHeader{
		ApiKey:        API_Heartbeat,
		ApiVersion:    0,
		CorrelationID: correlationID,
		ClientId:      clientID,
	}
	return &HeartbeatRequest{
		RequestHeader: requestHeader,
		GroupID:       groupID,
		GenerationID:  generationID,
		MemberID:      memberID,
	}
}

func (heartbeatR *HeartbeatRequest) Encode() []byte {
	requestLength := heartbeatR.RequestHeader.length() + 2 + len(heartbeatR.GroupID)

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = heartbeatR.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(heartbeatR.GroupID)))
	offset += 2
	copy(payload[offset:], heartbeatR.GroupID)

	binary.BigEndian.PutUint32(payload[offset:], uint32(heartbeatR.GenerationID))
	offset += 4

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(heartbeatR.MemberID)))
	offset += 2
	copy(payload[offset:], heartbeatR.MemberID)

	return payload
}
