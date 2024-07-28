package healer

import (
	"encoding/binary"
	"fmt"
)

//JoinGroup Response (Version: 0) => error_code generation_id group_protocol leader_id member_id [members]
//error_code => INT16
//generation_id => INT32
//group_protocol => STRING
//leader_id => STRING
//member_id => STRING
//members => member_id member_metadata
//member_id => STRING
//member_metadata => BYTES

//FIELD	DESCRIPTION
//error_code	Response error code
//generation_id	The generation of the group.
//group_protocol	The group protocol selected by the coordinator
//leader_id	The leader of the group
//member_id	The member id assigned by the group coordinator or null if joining for the first time.
//members	null
//member_id	The member id assigned by the group coordinator or null if joining for the first time.
//member_metadata	null

// version 0
type Member struct {
	MemberID       string
	MemberMetadata []byte
}
type JoinGroupResponse struct {
	CorrelationID uint32
	ErrorCode     int16
	GenerationID  int32
	GroupProtocol string
	LeaderID      string
	MemberID      string
	Members       []Member
}

func (r JoinGroupResponse) Error() error {
	return getErrorFromErrorCode(r.ErrorCode)
}

func NewJoinGroupResponse(payload []byte) (r JoinGroupResponse, err error) {
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("joingroup response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if r.ErrorCode != 0 {
		err = KafkaError(r.ErrorCode)
	}

	r.GenerationID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	groupProtocolLength := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	r.GroupProtocol = string(payload[offset : offset+groupProtocolLength])
	offset += groupProtocolLength

	LeaderIDLength := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	r.LeaderID = string(payload[offset : offset+LeaderIDLength])
	offset += LeaderIDLength

	memberIDLength := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	r.MemberID = string(payload[offset : offset+memberIDLength])
	offset += memberIDLength

	membersLength := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	r.Members = make([]Member, membersLength)
	for i := 0; i < membersLength; i++ {
		l := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		r.Members[i].MemberID = string(payload[offset : offset+l])
		offset += l

		ll := int(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		r.Members[i].MemberMetadata = make([]byte, ll)
		copy(r.Members[i].MemberMetadata, payload[offset:offset+ll])
		offset += ll
	}

	return r, err
}
