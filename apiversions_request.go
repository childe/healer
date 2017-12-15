package healer

import "encoding/binary"

type ApiVersionsRequest struct {
	RequestHeader *RequestHeader
}

var funcs []func(uint32, string) *ApiVersionsRequest

func init() {
	funcs = []func(uint32, string) *ApiVersionsRequest{newApiVersionsRequestV0, newApiVersionsRequestV1}
}

func NewApiVersionsRequest(apiVersion int, correlationID uint32, clientID string) Request {
	return funcs[apiVersion](correlationID, clientID)
}

func newApiVersionsRequestV0(correlationID uint32, clientID string) *ApiVersionsRequest {
	requestHeader := &RequestHeader{
		ApiKey:        API_ApiVersions,
		ApiVersion:    0,
		CorrelationID: correlationID,
		ClientId:      clientID,
	}

	return &ApiVersionsRequest{
		RequestHeader: requestHeader,
	}
}

func newApiVersionsRequestV1(correlationID uint32, clientID string) *ApiVersionsRequest {
	requestHeader := &RequestHeader{
		ApiKey:        API_ApiVersions,
		ApiVersion:    1,
		CorrelationID: correlationID,
		ClientId:      clientID,
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
