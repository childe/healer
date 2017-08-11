package healer

import "encoding/binary"

type ApiVersionsRequest struct {
	RequestHeader *RequestHeader
}

var funcs []func(int32, string) *ApiVersionsRequest

func init() {
	funcs = []func(int32, string) *ApiVersionsRequest{newApiVersionsRequestV0, newApiVersionsRequestV1}
}

func NewApiVersionsRequest(apiVersion int, correlationID int32, clientID string) Request {
	return funcs[apiVersion](correlationID, clientID)
}

func newApiVersionsRequestV0(correlationID int32, clientID string) *ApiVersionsRequest {
	requestHeader := &RequestHeader{
		ApiKey:        API_ApiVersions,
		ApiVersion:    0,
		CorrelationId: correlationID,
		ClientId:      clientID,
	}

	return &ApiVersionsRequest{
		RequestHeader: requestHeader,
	}
}

func newApiVersionsRequestV1(correlationID int32, clientID string) *ApiVersionsRequest {
	requestHeader := &RequestHeader{
		ApiKey:        API_ApiVersions,
		ApiVersion:    1,
		CorrelationId: correlationID,
		ClientId:      clientID,
	}

	return &ApiVersionsRequest{
		RequestHeader: requestHeader,
	}
}

func (apiVersionsRequest *ApiVersionsRequest) Encode() []byte {
	payload := make([]byte, apiVersionsRequest.RequestHeader.length()+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(apiVersionsRequest.RequestHeader.length()))
	offset += 4

	apiVersionsRequest.RequestHeader.Encode(payload, offset)
	return payload
}
