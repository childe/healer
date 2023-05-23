package healer

import (
	"encoding/binary"
	"fmt"
)

//ListGroups Response (Version: 0) => error_code [groups]
//error_code => INT16
//groups => group_id protocol_type
//group_id => STRING
//protocol_type => STRING

//FIELD	DESCRIPTION
//error_code	Response error code
//groups	null
//group_id	The unique group identifier
//protocol_type	null

// version 0
type Group struct {
	GroupID      string
	ProtocolType string
}
type ListGroupsResponse struct {
	CorrelationID uint32
	ErrorCode     uint16
	Groups        []*Group
}

func (r ListGroupsResponse) Error() error {
	return nil
}

func NewListGroupsResponse(payload []byte) (*ListGroupsResponse, error) {
	r := &ListGroupsResponse{}
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))

	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("ListGroups reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ErrorCode = binary.BigEndian.Uint16(payload[offset:])
	offset += 2

	groupCount := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	r.Groups = make([]*Group, groupCount)
	for i := 0; i < groupCount; i++ {
		r.Groups[i] = &Group{}
		l := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		r.Groups[i].GroupID = string(payload[offset : offset+l])
		offset += l

		ll := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		r.Groups[i].ProtocolType = string(payload[offset : offset+ll])
		offset += ll
	}

	return r, nil
}
