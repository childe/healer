package healer

import (
	"encoding/binary"
	"fmt"
)

// SaslHandshakeResponse is the response of saslhandshake request
type SaslHandshakeResponse struct {
	CorrelationID     uint32
	ErrorCode         int16
	EnabledMechanisms []string
}

func (r SaslHandshakeResponse) Error() error {
	return getErrorFromErrorCode(r.ErrorCode)
}

// NewSaslHandshakeResponse create a NewSaslHandshakeResponse instance from response payload bytes
func NewSaslHandshakeResponse(payload []byte) (r SaslHandshakeResponse, err error) {
	var offset = 0

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("SaslHandshakeResponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ErrorCode = int16(uint16(binary.BigEndian.Uint32(payload[offset:])))
	offset += 2
	if r.ErrorCode != 0 {
		err = getErrorFromErrorCode(r.ErrorCode)
	}

	count := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.EnabledMechanisms = make([]string, count)
	for i := 0; i < count; i++ {
		l := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		mechanism := string(payload[offset : offset+l])
		r.EnabledMechanisms[i] = mechanism
		offset += l
	}

	return r, err
}
