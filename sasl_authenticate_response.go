package healer

import (
	"encoding/binary"
	"fmt"
)

/*
SaslAuthenticate Response (Version: 0) => error_code error_message sasl_auth_bytes
  error_code => INT16
  error_message => NULLABLE_STRING
  sasl_auth_bytes => BYTES

FIELD	DESCRIPTION
error_code	Response error code
error_message	Response error message
sasl_auth_bytes	SASL authentication bytes from server as defined by the SASL mechanism.
*/

type SaslAuthenticateResponse struct {
	CorrelationID uint32
	ErrorCode     int16
	ErrorMessage  string
	SaslAuthBytes []byte
}

func (r SaslAuthenticateResponse) Error() error {
	return getErrorFromErrorCode(r.ErrorCode)
}

func NewSaslAuthenticateResponse(payload []byte) (*SaslAuthenticateResponse, error) {
	var (
		r      = &SaslAuthenticateResponse{}
		offset = 0
		err    error
	)

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("SaslAuthenticateResponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ErrorCode = int16(uint16(binary.BigEndian.Uint16(payload[offset:])))
	offset += 2
	if r.ErrorCode != 0 {
		err = getErrorFromErrorCode(r.ErrorCode)
	}

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
