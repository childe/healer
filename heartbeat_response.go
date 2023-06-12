package healer

import (
	"encoding/binary"
	"fmt"
)

type HeartbeatResponse struct {
	CorrelationID uint32
	ErrorCode     int16
}

func (r HeartbeatResponse) Error() error {
	return getErrorFromErrorCode(r.ErrorCode)
}

func NewHeartbeatResponse(payload []byte) (r HeartbeatResponse, err error) {
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("heartbeat response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))

	if r.ErrorCode != 0 {
		err = getErrorFromErrorCode(r.ErrorCode)
	}

	return r, err
}
