package healer

import (
	"encoding/binary"
	"fmt"
)

// version 0
type HeartbeatResponse struct {
	CorrelationID uint32
	ErrorCode     uint16
}

func NewHeartbeatResponse(payload []byte) (*HeartbeatResponse, error) {
	var err error = nil
	heartbeatResponse := &HeartbeatResponse{}
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("heartbeat reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	heartbeatResponse.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	heartbeatResponse.ErrorCode = binary.BigEndian.Uint16(payload[offset:])

	if heartbeatResponse.ErrorCode != 0 {
		err = AllError[heartbeatResponse.ErrorCode]
	}

	return heartbeatResponse, err
}
