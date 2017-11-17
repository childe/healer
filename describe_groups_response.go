package healer

import (
	"encoding/binary"
	"fmt"
)

/*
DescribeGroups Response (Version: 0) => [groups]
  groups => error_code group_id state protocol_type protocol [members]
    error_code => INT16
    group_id => STRING
    state => STRING
    protocol_type => STRING
    protocol => STRING
    members => member_id client_id client_host member_metadata member_assignment
      member_id => STRING
      client_id => STRING
      client_host => STRING
      member_metadata => BYTES
      member_assignment => BYTES

FIELD	DESCRIPTION
groups	null
error_code	Response error code
group_id	The unique group identifier
state	The current state of the group (one of: Dead, Stable, AwaitingSync, PreparingRebalance, or empty if there is no active group)
protocol_type	The current group protocol type (will be empty if there is no active group)
protocol	The current group protocol (only provided if the group is Stable)
members	Current group members (only provided if the group is not Dead)
member_id	The member id assigned by the group coordinator or null if joining for the first time.
client_id	The client id used in the member's latest join group request
client_host	The client host used in the request session corresponding to the member's join group.
member_metadata	The metadata corresponding to the current group protocol in use (will only be present if the group is stable).
member_assignment	The current assignment provided by the group leader (will only be present if the group is stable).
*/

// version 0
type MemberDetail struct {
	MemberID         string
	ClientID         string
	ClientHost       string
	MemberMetadata   []byte
	MemberAssignment []byte
}

type GroupDetail struct {
	ErrorCode    uint16
	GroupID      string
	State        string
	ProtocolType string
	Protocol     string
	Members      []*MemberDetail
}

type DescribeGroupsResponse struct {
	CorrelationID uint32
	Groups        []*GroupDetail
}

// TODO only return first error
func NewDescribeGroupsResponse(payload []byte) (*DescribeGroupsResponse, error) {
	var (
		err    error = nil
		l      int
		offset int = 0
	)
	r := &DescribeGroupsResponse{}
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("describeGroups reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	groupCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.Groups = make([]*GroupDetail, groupCount)

	for i := range r.Groups {
		group := &GroupDetail{}

		group.ErrorCode = binary.BigEndian.Uint16(payload[offset:])
		offset += 2

		if group.ErrorCode != 0 {
			err = AllError[group.ErrorCode]
		}

		l = int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		group.GroupID = string(payload[offset : offset+l])
		offset += l

		l = int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		group.State = string(payload[offset : offset+l])
		offset += l

		l = int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		group.ProtocolType = string(payload[offset : offset+l])
		offset += l

		l = int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		group.Protocol = string(payload[offset : offset+l])
		offset += l

		memberCount := int(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4

		group.Members = make([]*MemberDetail, memberCount)
		//MemberID         string
		//ClientID         string
		//ClientHost       string
		//MemberMetadata   []byte
		//MemberAssignment []byte
		for i := range group.Members {
			group.Members[i] = &MemberDetail{}

			l = int(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2
			group.Members[i].MemberID = string(payload[offset : offset+l])
			offset += l

			l = int(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2
			group.Members[i].ClientID = string(payload[offset : offset+l])
			offset += l

			l = int(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2
			group.Members[i].ClientHost = string(payload[offset : offset+l])
			offset += l

			l = int(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			copy(group.Members[i].MemberMetadata, payload[offset:offset+l])
			offset += l

			l = int(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			copy(group.Members[i].MemberAssignment, payload[offset:offset+l])
			offset += l
		}

		r.Groups[i] = group
	}

	return r, err
}
