package healer

import (
	"encoding/binary"
	"fmt"
)

// SaslAuthenticateResponse is the response of saslauthenticate request
type SaslAuthenticateResponse struct {
	CorrelationID uint32
	ErrorCode     int16
	ErrorMessage  string
	SaslAuthBytes []byte
}

func (r SaslAuthenticateResponse) Error() error {
	return getErrorFromErrorCode(r.ErrorCode)
}

// NewSaslAuthenticateResponse create a NewSaslAuthenticateResponse instance from response payload bytes
func NewSaslAuthenticateResponse(payload []byte) (r SaslAuthenticateResponse, err error) {
	var offset = 0

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("SaslAuthenticateResponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ErrorCode = int16(uint16(binary.BigEndian.Uint16(payload[offset:])))
	offset += 2

	l := int(int16(binary.BigEndian.Uint16(payload[offset:])))
	offset += 2
	if l > 0 {
		r.ErrorMessage = string(payload[offset : offset+l])
		offset += l
	}

	l = int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	r.SaslAuthBytes = make([]byte, l)
	copy(r.SaslAuthBytes, payload[offset:])

	return r, err
}
