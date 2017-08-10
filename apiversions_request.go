package healer

import "encoding/binary"

type ApiVersionsRequest struct {
	RequestHeader *RequestHeader
}

func NewApiVersionsRequest(correlationID int32, clientID string) *ApiVersionsRequest {
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

func (apiVersionsRequest *ApiVersionsRequest) Encode() []byte {
	payload := make([]byte, apiVersionsRequest.RequestHeader.length()+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(apiVersionsRequest.RequestHeader.length()))
	offset += 4

	apiVersionsRequest.RequestHeader.Encode(payload, offset)
	return payload
}
