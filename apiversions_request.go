package healer

import "encoding/binary"

type ApiVersionsRequest struct {
	*RequestHeader
}

func NewApiVersionsRequest(apiVersion uint16, clientID string) Request {
	requestHeader := &RequestHeader{
		APIKey:     API_ApiVersions,
		APIVersion: apiVersion,
		ClientID:   clientID,
	}

	return &ApiVersionsRequest{
		RequestHeader: requestHeader,
	}
}

func (req *ApiVersionsRequest) Encode() []byte {
	payload := make([]byte, req.RequestHeader.length()+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(req.RequestHeader.length()))
	offset += 4

	req.RequestHeader.Encode(payload, offset)
	return payload
}
