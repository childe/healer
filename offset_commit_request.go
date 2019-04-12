package healer

//TODO v1
/*
OffsetCommit Request (Version: 0) => group_id [topics]
  group_id => STRING
  topics => topic [partitions]
    topic => STRING
    partitions => partition offset metadata
      partition => INT32
      offset => INT64
      metadata => NULLABLE_STRING

OffsetCommit Request (Version: 1) => group_id generation_id member_id [topics]
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  topics => topic [partitions]
    topic => STRING
    partitions => partition offset timestamp metadata
      partition => INT32
      offset => INT64
      timestamp => INT64
      metadata => NULLABLE_STRING

OffsetCommit Request (Version: 2) => group_id generation_id member_id retention_time [topics]
  group_id => STRING
  generation_id => INT32
  member_id => STRING
  retention_time => INT64
  topics => topic [partitions]
    topic => STRING
    partitions => partition offset metadata
      partition => INT32
      offset => INT64
      metadata => NULLABLE_STRING

group_id	The unique group identifier
generation_id	The generation of the group.
member_id	The member id assigned by the group coordinator or null if joining for the first time.
retention_time	Time period in ms to retain the offset.
topics	Topics to commit offsets.
topic	Name of topic
partitions	Partitions to commit offsets.
partition	Topic partition id
offset	Message offset to be committed.
metadata	Any associated metadata the client wants to keep.*/

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
	RequestHeader *RequestHeader
	GroupID       string
	GenerationID  int32
	MemberID      string
	RetentionTime int64
	Topics        []*OffsetCommitRequestTopic
}

// request only ONE topic
func NewOffsetCommitRequest(apiVersion uint16, clientID, groupID string) *OffsetCommitRequest {
	requestHeader := &RequestHeader{
		ApiKey:     API_OffsetCommitRequest,
		ApiVersion: apiVersion,
		ClientId:   clientID,
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

	if r.APIVersion() == 2 {
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
func (r *OffsetCommitRequest) Encode() []byte {
	requestLength := r.Length()
	payload := make([]byte, 4+requestLength)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = r.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.GroupID)))
	offset += 2
	offset += copy(payload[offset:], r.GroupID)

	if r.APIVersion() == 2 {
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

func (req *OffsetCommitRequest) APIVersion() uint16 {
	return req.RequestHeader.ApiVersion
}

func (req *OffsetCommitRequest) API() uint16 {
	return req.RequestHeader.ApiKey
}

func (req *OffsetCommitRequest) SetCorrelationID(c uint32) {
	req.RequestHeader.CorrelationID = c
}
