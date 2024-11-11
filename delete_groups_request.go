package healer

import (
	"encoding/binary"
)

// DeleteGroupsRequest Request holds the argument of DeleteGroupsRequest
type DeleteGroupsRequest struct {
	*RequestHeader
	GroupsNames []string `json:"groups_names"`
}

// NewDeleteGroupsRequest creates a new DeleteGroupsRequest
func NewDeleteGroupsRequest(clientID string, groupsNames []string) DeleteGroupsRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_Delete_Groups,
		APIVersion: 0,
		ClientID:   &clientID,
	}
	return DeleteGroupsRequest{
		RequestHeader: requestHeader,
		GroupsNames:   groupsNames,
	}
}

func (r DeleteGroupsRequest) length() int {
	requestLength := r.RequestHeader.length() + 4
	for _, groupName := range r.GroupsNames {
		requestLength += 2 + len(groupName)
	}
	return requestLength
}

// Encode encodes DeleteGroupsRequest to []byte
func (r DeleteGroupsRequest) Encode(version uint16) []byte {
	requestLength := r.length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += r.RequestHeader.EncodeTo(payload[offset:])

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.GroupsNames)))
	offset += 4

	for _, groupName := range r.GroupsNames {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(groupName)))
		offset += 2
		offset += copy(payload[offset:], groupName)
	}

	return payload
}
