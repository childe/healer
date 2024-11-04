package healer

import "encoding/binary"

type SyncGroupRequest struct {
	*RequestHeader
	GroupID         string
	GenerationID    int32
	MemberID        string
	GroupAssignment GroupAssignment
}

func NewSyncGroupRequest(clientID, groupID string, generationID int32, memberID string, groupAssignment GroupAssignment) *SyncGroupRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_SyncGroup,
		APIVersion: 0,
		ClientID:   &clientID,
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

// Encode encodes SyncGroupRequest to []byte
func (r *SyncGroupRequest) Encode(version uint16) []byte {
	requestLength := r.Length()
	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += r.RequestHeader.Encode(payload[offset:])

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.GroupID)))
	offset += 2
	offset += copy(payload[offset:], r.GroupID)

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.GenerationID))
	offset += 4

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.MemberID)))
	offset += 2
	offset += copy(payload[offset:], r.MemberID)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.GroupAssignment)))
	offset += 4
	for _, x := range r.GroupAssignment {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(x.MemberID)))
		offset += 2
		offset += copy(payload[offset:], x.MemberID)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(x.MemberAssignment)))
		offset += 4
		offset += copy(payload[offset:], x.MemberAssignment)
	}

	return payload
}
