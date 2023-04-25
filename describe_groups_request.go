package healer

import (
	"encoding/binary"
)

// DescribeGroupsRequest holds the parameters for the DescribeGroups request API
type DescribeGroupsRequest struct {
	*RequestHeader
	Groups []string
}

// NewDescribeGroupsRequest creates a new DescribeGroupsRequest
func NewDescribeGroupsRequest(clientID string, groups []string) *DescribeGroupsRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_DescribeGroups,
		APIVersion: 0,
		ClientID:   clientID,
	}
	return &DescribeGroupsRequest{requestHeader, groups}
}

func (r *DescribeGroupsRequest) length() int {
	l := r.RequestHeader.length()
	l += 4
	for _, group := range r.Groups {
		l += 2 + len(group)
	}
	return l
}

// Encode encodes the request into byte array, this implements the Request interface
func (r *DescribeGroupsRequest) Encode(version uint16) []byte {
	requestLength := r.length()

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
