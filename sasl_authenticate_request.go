package healer

import (
	"encoding/binary"
	"strings"

	"github.com/golang/glog"
)

type SaslAuth interface {
	Encode() []byte
}

/*
SaslAuthenticate API (Key: 36):

Requests:
SaslAuthenticate Request (Version: 0) => sasl_auth_bytes
  sasl_auth_bytes => BYTES

FIELD	DESCRIPTION
sasl_auth_bytes	SASL authentication bytes from client as defined by the SASL mechanism.
*/

// version0
type SaslAuthenticateRequest struct {
	*RequestHeader
	SaslAuthBytes []byte
}

func NewSaslAuthenticateRequest(clientID string, user, password, typ string) *SaslAuthenticateRequest {
	var saslAuth SaslAuth
	switch strings.ToLower(typ) {
	case "plain":
		saslAuth = NewPlainSasl(user, password)
	default:
		glog.Errorf("%s NOT support for now", typ)
		return nil
	}

	requestHeader := &RequestHeader{
		APIKey:     API_SaslAuthenticate,
		APIVersion: 0,
		ClientID:   clientID,
	}
	saslAuthBytes := saslAuth.Encode()
	return &SaslAuthenticateRequest{requestHeader, saslAuthBytes}
}

func (r *SaslAuthenticateRequest) Length() int {
	l := r.RequestHeader.length()
	return l + 4 + len(r.SaslAuthBytes)
}

func (r *SaslAuthenticateRequest) Encode(version uint16) []byte {
	requestLength := r.Length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = r.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.SaslAuthBytes)))
	offset += 4

	copy(payload[offset:], r.SaslAuthBytes)

	return payload
}
