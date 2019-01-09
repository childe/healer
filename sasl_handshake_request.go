package healer

import (
	"encoding/binary"
)

/*
SaslHandshake API (Key: 17):

Requests:
SaslHandshake Request (Version: 0) => mechanism
  mechanism => STRING

FIELD	DESCRIPTION
mechanism	SASL Mechanism chosen by the client.

=== === === ===

SaslHandshake Request (Version: 1) => mechanism
  mechanism => STRING

FIELD	DESCRIPTION
mechanism	SASL Mechanism chosen by the client.
*/

// version0
type SaslHandShakeRequest struct {
	RequestHeader *RequestHeader
	Mechanism     string
}

func NewSaslHandShakeRequest(clientID string, mechanism string) *SaslHandShakeRequest {
	requestHeader := &RequestHeader{
		ApiKey:     API_SaslHandshake,
		ApiVersion: 1,
		ClientId:   clientID,
	}
	return &SaslHandShakeRequest{requestHeader, mechanism}
}

func (r *SaslHandShakeRequest) Length() int {
	l := r.RequestHeader.length()
	return l + 2 + len(r.Mechanism)
}

func (r *SaslHandShakeRequest) Encode() []byte {
	requestLength := r.Length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = r.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.Mechanism)))
	offset += 2

	copy(payload[offset:], r.Mechanism)

	return payload
}

func (req *SaslHandShakeRequest) API() uint16 {
	return req.RequestHeader.ApiKey
}

func (req *SaslHandShakeRequest) SetCorrelationID(c uint32) {
	req.RequestHeader.CorrelationID = c
}
