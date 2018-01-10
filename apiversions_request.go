package healer

import "encoding/binary"

type ApiVersionsRequest struct {
	RequestHeader *RequestHeader
}

func NewApiVersionsRequest(apiVersion uint16, clientID string) Request {
	requestHeader := &RequestHeader{
		ApiKey:     API_ApiVersions,
		ApiVersion: apiVersion,
		ClientId:   clientID,
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

func (req *ApiVersionsRequest) API() uint16 {
	return req.RequestHeader.ApiKey
}

func (req *ApiVersionsRequest) SetCorrelationID(c uint32) {
	req.RequestHeader.CorrelationID = c
}
