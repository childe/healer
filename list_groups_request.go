package healer

import (
	"encoding/binary"
)

// version0
type ListGroupsRequest struct {
	RequestHeader *RequestHeader
}

func NewListGroupsRequest(clientID string) *ListGroupsRequest {
	requestHeader := &RequestHeader{
		ApiKey:     API_ListGroups,
		ApiVersion: 0,
		ClientId:   clientID,
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

func (req *ListGroupsRequest) API() uint16 {
	return req.RequestHeader.ApiKey
}

func (req *ListGroupsRequest) SetCorrelationID(c uint32) {
	req.RequestHeader.CorrelationID = c
}
