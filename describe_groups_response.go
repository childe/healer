package healer

import (
	"encoding/binary"
	"fmt"
)

type MemberDetail struct {
	MemberID         string
	ClientID         string
	ClientHost       string
	MemberMetadata   []byte
	MemberAssignment []byte
}

type GroupDetail struct {
	ErrorCode    int16
	GroupID      string
	State        string
	ProtocolType string
	Protocol     string
	Members      []MemberDetail
}

type DescribeGroupsResponse struct {
	CorrelationID uint32
	Groups        []*GroupDetail
}

func (r DescribeGroupsResponse) Error() error {
	for _, group := range r.Groups {
		if group.ErrorCode != 0 {
			return getErrorFromErrorCode(group.ErrorCode)
		}
	}
	return nil
}

func NewDescribeGroupsResponse(payload []byte) (r DescribeGroupsResponse, err error) {
	var (
		l      int
		offset int = 0
	)
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("describeGroups response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	groupCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.Groups = make([]*GroupDetail, groupCount)

	for i := range r.Groups {
		group := &GroupDetail{}

		group.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		if err == nil && group.ErrorCode != 0 {
			err = getErrorFromErrorCode(group.ErrorCode)
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

		group.Members = make([]MemberDetail, memberCount)

		for i := range group.Members {
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
			group.Members[i].MemberAssignment = make([]byte, l)
			copy(group.Members[i].MemberAssignment, payload[offset:offset+l])
			offset += l
		}

		r.Groups[i] = group
	}

	return r, err
}
