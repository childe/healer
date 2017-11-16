package healer

import (
	"encoding/binary"
)

/*
The sync group request is used by the group leader to assign state (e.g. partition assignments) to
all members of the current generation. All members send SyncGroup immediately after joining the group,
but only the leader provides the group's assignment.
*/

/*
Consumer Groups: The format of the MemberAssignment field for consumer groups is included below:
MemberAssignment => Version PartitionAssignment
  Version => int16
  PartitionAssignment => [Topic [Partition]]
    Topic => string
    Partition => int32
  UserData => bytes
All client implementations using the "consumer" protocol type should support this schema.
*/

/*
SyncGroup Request (Version: 0) => group_id generation_id member_id [group_assignment]
group_id => STRING
generation_id => INT32
member_id => STRING
group_assignment => member_id member_assignment
member_id => STRING
member_assignment => BYTES

FIELD	DESCRIPTION
group_id	The unique group identifier
generation_id	The generation of the group.
member_id	The member id assigned by the group coordinator or null if joining for the first time.
group_assignment	null
member_id	The member id assigned by the group coordinator or null if joining for the first time.
member_assignment	null
*/

// TODO version0
type GroupAssignment struct {
	MemberID         string
	MemberAssignment []byte
}

type SyncGroupRequest struct {
	RequestHeader     *RequestHeader
	GroupID           string
	GroupGenerationID int32
	MemberID          string
	GroupAssignments  []*GroupAssignment
}

func NewSyncGroupRequest(correlationID uint32, clientID, groupID string,
	groupGenerationID int32, memberID string) *SyncGroupRequest {
	requestHeader := &RequestHeader{
		ApiKey:        API_SyncGroup,
		ApiVersion:    0,
		CorrelationID: correlationID,
		ClientId:      clientID,
	}

	return &SyncGroupRequest{
		RequestHeader:     requestHeader,
		GroupID:           groupID,
		GroupGenerationID: groupGenerationID,
		MemberID:          memberID,
		GroupAssignments:  make([]*GroupAssignment, 0),
	}
}

func (r *SyncGroupRequest) AddGroupProtocal(memberID string, memberAssignment []byte) {
	r.GroupAssignments = append(r.GroupAssignments, &GroupAssignment{memberID, memberAssignment})
}

func (r *SyncGroupRequest) Length() int {
	requestLength := r.RequestHeader.length() + 2 + len(r.GroupID) + 4 + 2 + len(r.MemberID)
	requestLength += 4
	for _, ga := range r.GroupAssignments {
		requestLength += 2 + len(ga.MemberID)
		requestLength += 4 + len(ga.MemberAssignment)
	}
	return requestLength
}

func (r *SyncGroupRequest) Encode() []byte {
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

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.GroupGenerationID))
	offset += 4

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.MemberID)))
	offset += 2
	copy(payload[offset:], r.MemberID)
	offset += len(r.MemberID)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.GroupAssignments)))
	offset += 4
	for _, ga := range r.GroupAssignments {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(ga.MemberID)))
		offset += 2
		copy(payload[offset:], ga.MemberID)
		offset += len(ga.MemberID)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(ga.MemberID)))
		offset += 4
		copy(payload[offset:], ga.MemberAssignment)
		offset += len(ga.MemberID)
	}

	return payload
}
