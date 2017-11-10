package healer

import (
	"encoding/binary"
)

// version0
type ListGroupsRequest struct {
	RequestHeader *RequestHeader
}

func NewListGroupsRequest(correlationID int32, clientID string) *ListGroupsRequest {
	requestHeader := &RequestHeader{
		ApiKey:        API_ListGroups,
		ApiVersion:    0,
		CorrelationId: correlationID,
		ClientId:      clientID,
	}
	return &ListGroupsRequest{requestHeader}
}

func (ListGroupsR *ListGroupsRequest) Encode() []byte {
	requestLength := ListGroupsR.RequestHeader.length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	ListGroupsR.RequestHeader.Encode(payload, offset)

	return payload
}
