package healer

import (
	"encoding/binary"
)

// version0
type ListGroupsRequest struct {
	*RequestHeader
}

func NewListGroupsRequest(clientID string) *ListGroupsRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_ListGroups,
		APIVersion: 0,
		ClientID:   &clientID,
	}
	return &ListGroupsRequest{requestHeader}
}

func (ListGroupsR *ListGroupsRequest) Encode(version uint16) []byte {
	requestLength := ListGroupsR.RequestHeader.length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	ListGroupsR.RequestHeader.EncodeTo(payload[offset:])

	return payload
}
