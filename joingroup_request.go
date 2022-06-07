package healer

import (
	"encoding/binary"
)

/*
https://kafka.apache.org/protocol.html#The_Messages_JoinGroup
*/

// JoinGroupRequest struct holds params in JoinGroupRequest
type JoinGroupRequest struct {
	*RequestHeader
	GroupID          string
	SessionTimeout   int32 // ms
	RebalanceTimeout int32 // ms. this is NOT included in verions 0
	MemberID         string
	ProtocolType     string
	GroupProtocols   []*GroupProtocol
}

// GroupProtocol is sub struct in JoinGroupRequest
type GroupProtocol struct {
	ProtocolName     string
	ProtocolMetadata []byte
}

// NewJoinGroupRequest create a JoinGroupRequest
func NewJoinGroupRequest(apiVersion uint16, clientID string) *JoinGroupRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_JoinGroup,
		APIVersion: apiVersion,
		ClientID:   clientID,
	}

	return &JoinGroupRequest{
		RequestHeader:  requestHeader,
		GroupProtocols: []*GroupProtocol{},
	}
}

// AddGroupProtocal add new GroupProtocol to JoinGroupReuqest
func (r *JoinGroupRequest) AddGroupProtocal(gp *GroupProtocol) {
	r.GroupProtocols = append(r.GroupProtocols, gp)
}

func (r *JoinGroupRequest) length() int {
	l := r.RequestHeader.length() + 2 + len(r.GroupID) + 4 + 2 + len(r.MemberID) + 2 + len(r.ProtocolType)
	if r.RequestHeader.APIVersion == 1 || r.RequestHeader.APIVersion == 2 {
		l += 4 // RebalanceTimeout
	}
	l += 4
	for _, gp := range r.GroupProtocols {
		l += 2 + len(gp.ProtocolName)
		l += 4 + len(gp.ProtocolMetadata)
	}
	return l
}

// Encode encodes the JoinGroupRequest object to []byte. it implement Request Interface
func (r *JoinGroupRequest) Encode() []byte {
	requestLength := r.length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = r.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.GroupID)))
	offset += 2
	offset += copy(payload[offset:], r.GroupID)

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.SessionTimeout))
	offset += 4

	if r.Version() == 1 || r.Version() == 2 {
		binary.BigEndian.PutUint32(payload[offset:], uint32(r.RebalanceTimeout))
		offset += 4
	}

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.MemberID)))
	offset += 2
	offset += copy(payload[offset:], r.MemberID)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.ProtocolType)))
	offset += 2
	offset += copy(payload[offset:], r.ProtocolType)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.GroupProtocols)))
	offset += 4

	for _, gp := range r.GroupProtocols {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(gp.ProtocolName)))
		offset += 2
		offset += copy(payload[offset:], gp.ProtocolName)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(gp.ProtocolMetadata)))
		offset += 4
		offset += copy(payload[offset:], gp.ProtocolMetadata)
	}
	return payload
}
