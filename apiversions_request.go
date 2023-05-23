package healer

import "encoding/binary"

type APIVersionsRequest struct {
	*RequestHeader
}

func NewApiVersionsRequest(clientID string) Request {
	requestHeader := &RequestHeader{
		APIKey:   API_ApiVersions,
		ClientID: clientID,
	}

	return &APIVersionsRequest{
		RequestHeader: requestHeader,
	}
}

// Encode encodes ApiVersionsRequest to []byte
func (req *APIVersionsRequest) Encode(version uint16) []byte {
	payload := make([]byte, req.RequestHeader.length()+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(req.RequestHeader.length()))
	offset += 4

	req.RequestHeader.Encode(payload[offset:])
	return payload
}
