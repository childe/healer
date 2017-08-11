package healer

import "encoding/binary"

type ApiVersionsRequest struct {
	RequestHeader *RequestHeader
}

func NewApiVersionsRequest(apiVersion uint16, correlationID int32, clientID string) Request {
	if apiVersion == 0 {
		return newApiVersionsRequestV0(correlationID, clientID)
	}
	return newApiVersionsRequestV1(correlationID, clientID)
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
