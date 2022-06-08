package healer

import (
	"encoding/binary"
)

/*
DescribeGroups Request (Version: 0) => [group_ids]
  group_ids => STRING

FIELDDESCRIPTION
  group_idsList of groupIds to request metadata for (an empty groupId array will return empty group metadata).
*/

// version0
type DescribeGroupsRequest struct {
	*RequestHeader
	Groups []string
}

func NewDescribeGroupsRequest(clientID string, groups []string) *DescribeGroupsRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_DescribeGroups,
		APIVersion: 0,
		ClientID:   clientID,
	}
	return &DescribeGroupsRequest{requestHeader, groups}
}

func (r *DescribeGroupsRequest) Length() int {
	l := r.RequestHeader.length()
	l += 4
	for _, group := range r.Groups {
		l += 2 + len(group)
	}
	return l
}

func (r *DescribeGroupsRequest) Encode(version uint16) []byte {
	requestLength := r.Length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += r.RequestHeader.Encode(payload[offset:])

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Groups)))
	offset += 4

	for _, group := range r.Groups {
		l := len(group)
		binary.BigEndian.PutUint16(payload[offset:], uint16(l))
		offset += 2
		copy(payload[offset:offset+l], group)
		offset += l
	}

	return payload
}
