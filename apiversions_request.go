package healer

import (
	"encoding/binary"
)

type ApiVersionRequest struct {
	RequestHeader *RequestHeader
}

func NewApiVersionsRequest(correlationID int32, clientID string) *FetchRequest {
	requestHeader := &RequestHeader{
		ApiKey:        API_ApiVersions,
		ApiVersion:    0,
		CorrelationId: correlationID,
		ClientId:      clientID,
	}

	return &FetchRequest{
		RequestHeader: requestHeader,
	}
}

func (apiVersionRequest *ApiVersionRequest) Encode() []byte {
	payload := make([]byte, apiVersionRequest.RequestHeader.length()+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(apiVersionRequest.RequestHeader.length()))
	offset += 4

	apiVersionRequest.RequestHeader.Encode(payload, offset)
	return payload
}
