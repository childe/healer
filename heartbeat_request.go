package healer

import (
	"encoding/binary"
)

// TODO version0
type HeartbeatRequest struct {
	RequestHeader     *RequestHeader
	GroupID           string
	groupGenerationID int32
	memberID          string
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

	binary.BigEndian.PutUint32(payload[offset:], uint32(heartbeatR.groupGenerationID))
	offset += 4

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(heartbeatR.memberID)))
	offset += 2
	copy(payload[offset:], heartbeatR.memberID)

	return payload
}
