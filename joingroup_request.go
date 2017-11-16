package healer

import (
	"encoding/binary"
)

//JoinGroup Request (Version: 0) => group_id session_timeout member_id protocol_type [group_protocols]
//group_id => STRING
//session_timeout => INT32
//member_id => STRING
//protocol_type => STRING
//group_protocols => protocol_name protocol_metadata
//protocol_name => STRING
//protocol_metadata => BYTES

//FIELD	DESCRIPTION
//group_id	The unique group identifier
//session_timeout	The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms.
//member_id	The member id assigned by the group coordinator or null if joining for the first time.
//protocol_type	Unique name for class of protocols implemented by group
//group_protocols	List of protocols that the member supports
//protocol_name	null
//protocol_metadata	null

// version 0
type GroupProtocol struct {
	ProtocolName     string
	ProtocolMetadata []byte
}
type JoinGroupRequest struct {
	RequestHeader  *RequestHeader
	GroupID        string
	SessionTimeout int32
	MemberID       string
	ProtocolType   string
	GroupProtocols []*GroupProtocol
}

func NewJoinGroupRequest(
	correlationID uint32, clientID, groupID string, sessionTimeout int32,
	memberID, protocolType string) *JoinGroupRequest {
	requestHeader := &RequestHeader{
		ApiKey:        API_JoinGroup,
		ApiVersion:    0,
		CorrelationID: correlationID,
		ClientId:      clientID,
	}

	return &JoinGroupRequest{
		RequestHeader:  requestHeader,
		GroupID:        groupID,
		SessionTimeout: sessionTimeout,
		MemberID:       memberID,
		ProtocolType:   protocolType,
		GroupProtocols: make([]*GroupProtocol, 0),
	}
}
func (r *JoinGroupRequest) AddGroupProtocal(protocolName string, protocolMetadata []byte) {
	r.GroupProtocols = append(r.GroupProtocols, &GroupProtocol{protocolName, protocolMetadata})
}

func (r *JoinGroupRequest) Length() int {
	l := r.RequestHeader.length() + 2 + len(r.GroupID) + 4 + 2 + len(r.MemberID) + 2 + len(r.ProtocolType)
	l += 4
	for _, gp := range r.GroupProtocols {
		l += 2 + len(gp.ProtocolName)
		l += 4 + len(gp.ProtocolMetadata)
	}
	return l
}

func (r *JoinGroupRequest) Encode() []byte {
	requestLength := r.Length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = r.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.GroupID)))
	offset += 2
	copy(payload[offset:], r.GroupID)
	offset += len(r.GroupID)

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.SessionTimeout))
	offset += 4

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.MemberID)))
	offset += 2
	copy(payload[offset:], r.MemberID)
	offset += len(r.MemberID)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.ProtocolType)))
	offset += 2
	copy(payload[offset:], r.ProtocolType)
	offset += len(r.ProtocolType)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.GroupProtocols)))
	offset += 4

	for _, gp := range r.GroupProtocols {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(gp.ProtocolName)))
		offset += 2
		copy(payload[offset:], gp.ProtocolName)
		offset += len(gp.ProtocolName)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(gp.ProtocolMetadata)))
		offset += 4
		copy(payload[offset:], gp.ProtocolMetadata)
		offset += len(gp.ProtocolMetadata)
	}
	return payload
}
