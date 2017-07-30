package healer

import (
	"encoding/binary"
	"fmt"
)


// version 0
type HeartbeatReseponse struct {
	CorrelationId uint32
	ErrorCode     uint16
}

func (heartbeatReseponse *HeartbeatReseponse) Decode(payload []byte) error {
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return fmt.Errorf("heartbeat reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	heartbeatReseponse.CorrelationId = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	heartbeatReseponse.ErrorCode = binary.BigEndian.Uint16(payload[offset:])

	return nil
}
