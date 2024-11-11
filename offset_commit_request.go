package healer

import (
	"encoding/binary"
)

type OffsetCommitRequestPartition struct {
	PartitionID int32
	Offset      int64
	Metadata    string
}
type OffsetCommitRequestTopic struct {
	Topic      string
	Partitions []*OffsetCommitRequestPartition
}

type OffsetCommitRequest struct {
	*RequestHeader
	GroupID       string
	GenerationID  int32
	MemberID      string
	RetentionTime int64
	Topics        []*OffsetCommitRequestTopic
}

// request only ONE topic
func NewOffsetCommitRequest(apiVersion uint16, clientID, groupID string) *OffsetCommitRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_OffsetCommitRequest,
		APIVersion: apiVersion,
		ClientID:   &clientID,
	}

	r := &OffsetCommitRequest{
		RequestHeader: requestHeader,
		GroupID:       groupID,
	}

	r.Topics = make([]*OffsetCommitRequestTopic, 0)

	return r
}

func (r *OffsetCommitRequest) SetGenerationID(generationID int32) {
	r.GenerationID = generationID
}

func (r *OffsetCommitRequest) SetMemberID(memberID string) {
	r.MemberID = memberID
}

func (r *OffsetCommitRequest) SetRetentionTime(retentionTime int64) {
	r.RetentionTime = retentionTime
}

func (r *OffsetCommitRequest) AddPartiton(topic string, partitionID int32, offset int64, metadata string) {
	if r.Topics == nil {
		r.Topics = make([]*OffsetCommitRequestTopic, 0)
	}

	var theTopic *OffsetCommitRequestTopic = nil
	for _, t := range r.Topics {
		if t.Topic == topic {
			theTopic = t
			break
		}
	}
	if theTopic == nil {
		theTopic = &OffsetCommitRequestTopic{
			Topic:      topic,
			Partitions: make([]*OffsetCommitRequestPartition, 0),
		}
		r.Topics = append(r.Topics, theTopic)
	}

	for _, p := range theTopic.Partitions {
		if p.PartitionID == partitionID {
			p.Offset = offset
			p.Metadata = metadata
			return
		}
	}
	thePartition := &OffsetCommitRequestPartition{
		PartitionID: partitionID,
		Offset:      offset,
		Metadata:    metadata,
	}

	theTopic.Partitions = append(theTopic.Partitions, thePartition)
	return
}

func (r *OffsetCommitRequest) Length() int {
	l := r.RequestHeader.length()
	l += 2 + len(r.GroupID)

	if r.Version() == 2 {
		l += 4 + 2 + len(r.MemberID) + 8
	}

	l += 4
	for _, t := range r.Topics {
		l += 2 + len(t.Topic)
		l += 4
		for _, p := range t.Partitions {
			l += 4 + 8 + 2 + len(p.Metadata)
		}
	}
	return l
}
func (r *OffsetCommitRequest) Encode(version uint16) []byte {
	requestLength := r.Length()
	payload := make([]byte, 4+requestLength)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += r.RequestHeader.EncodeTo(payload[offset:])

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.GroupID)))
	offset += 2
	offset += copy(payload[offset:], r.GroupID)

	if r.Version() == 2 {
		binary.BigEndian.PutUint32(payload[offset:], uint32(r.GenerationID))
		offset += 4

		binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.MemberID)))
		offset += 2
		offset += copy(payload[offset:], r.MemberID)

		binary.BigEndian.PutUint64(payload[offset:], uint64(r.RetentionTime))
		offset += 8
	}

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Topics)))
	offset += 4

	for _, t := range r.Topics {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(t.Topic)))
		offset += 2

		offset += copy(payload[offset:], t.Topic)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(t.Partitions)))
		offset += 4
		for _, p := range t.Partitions {
			binary.BigEndian.PutUint32(payload[offset:], uint32(p.PartitionID))
			offset += 4

			binary.BigEndian.PutUint64(payload[offset:], uint64(p.Offset))
			offset += 8
			binary.BigEndian.PutUint16(payload[offset:], uint16(len(p.Metadata)))
			offset += 2
			offset += copy(payload[offset:], p.Metadata)
		}
	}

	return payload
}
