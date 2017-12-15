package healer

import "encoding/binary"

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
type SyncGroupRequest struct {
	RequestHeader   *RequestHeader
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupAssignment GroupAssignment
}

func NewSyncGroupRequest(correlationID uint32, clientID, groupID string,
	generationID int32, memberID string, groupAssignment GroupAssignment) *SyncGroupRequest {
	requestHeader := &RequestHeader{
		ApiKey:        API_SyncGroup,
		ApiVersion:    0,
		CorrelationID: correlationID,
		ClientId:      clientID,
	}

	return &SyncGroupRequest{
		RequestHeader:   requestHeader,
		GroupID:         groupID,
		GenerationID:    generationID,
		MemberID:        memberID,
		GroupAssignment: groupAssignment,
	}
}

func (r *SyncGroupRequest) Length() int {
	requestLength := r.RequestHeader.length() + 2 + len(r.GroupID) + 4 + 2 + len(r.MemberID)
	requestLength += 4
	for _, x := range r.GroupAssignment {
		requestLength += 2 + len(x.MemberID)
		requestLength += 4 + len(x.MemberAssignment)
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

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.GenerationID))
	offset += 4

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.MemberID)))
	offset += 2
	copy(payload[offset:], r.MemberID)
	offset += len(r.MemberID)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.GroupAssignment)))
	offset += 4
	for _, x := range r.GroupAssignment {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(x.MemberID)))
		offset += 2
		copy(payload[offset:], x.MemberID)
		offset += len(x.MemberID)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(x.MemberAssignment)))
		offset += 4
		copy(payload[offset:], x.MemberAssignment)
		offset += len(x.MemberID)
	}

	return payload
}

func (req *SyncGroupRequest) API() uint16 {
	return req.RequestHeader.ApiKey
}

func (req *SyncGroupRequest) SetCorrelationID(c uint32) {
	req.RequestHeader.CorrelationID = c
}
