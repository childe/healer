package healer

/*
OffsetFetch Request (Version: 0) => group_id [topics]
  group_id => STRING
  topics => topic [partitions]
    topic => STRING
    partitions => partition
      partition => INT32

group_id	The unique group identifier
topics	Topics to fetch offsets.
topic	Name of topic
partitions	Partitions to fetch offsets.
partition	Topic partition id
*/

import (
	"encoding/binary"
)

type OffsetFetchRequestTopic struct {
	Topic      string
	Partitions []int32
}

type OffsetFetchRequest struct {
	RequestHeader *RequestHeader
	GroupID       string
	Topics        []*OffsetFetchRequestTopic
}

// request only ONE topic
func NewOffsetFetchRequest(correlationID uint32, clientID, groupID string) *OffsetFetchRequest {
	requestHeader := &RequestHeader{
		ApiKey:        API_OffsetFetchRequest,
		ApiVersion:    0,
		CorrelationID: correlationID,
		ClientId:      clientID,
	}

	r := &OffsetFetchRequest{
		RequestHeader: requestHeader,
		GroupID:       groupID,
	}

	r.Topics = make([]*OffsetFetchRequestTopic, 0)

	return r
}

func (r *OffsetFetchRequest) AddPartiton(topic string, partitionID int32) {
	if r.Topics == nil {
		r.Topics = make([]*OffsetFetchRequestTopic, 0)
	}

	var theTopic *OffsetFetchRequestTopic = nil
	for _, t := range r.Topics {
		if t.Topic == topic {
			theTopic = t
			break
		}
	}
	if theTopic == nil {
		theTopic = &OffsetFetchRequestTopic{
			Topic: topic,
		}
		r.Topics = append(r.Topics, theTopic)
	}

	for _, p := range theTopic.Partitions {
		if p == partitionID {
			return
		}
	}
	theTopic.Partitions = append(theTopic.Partitions, partitionID)
	return
}

func (r *OffsetFetchRequest) Length() int {
	l := r.RequestHeader.length()
	l += 2 + len(r.GroupID)

	l += 4
	for _, t := range r.Topics {
		l += 2 + len(t.Topic)
		l += 4 + 4*len(t.Partitions)
	}
	return l
}

func (r *OffsetFetchRequest) Encode() []byte {
	requestLength := r.Length()
	payload := make([]byte, 4+requestLength)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = r.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.GroupID)))
	offset += 2
	copy(payload[offset:], r.GroupID)
	offset += len(r.GroupID)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Topics)))
	offset += 4

	for _, t := range r.Topics {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(t.Topic)))
		offset += 2

		copy(payload[offset:], t.Topic)
		offset += len(t.Topic)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(t.Partitions)))
		offset += 4
		for _, p := range t.Partitions {
			binary.BigEndian.PutUint32(payload[offset:], uint32(p))
			offset += 4
		}
	}

	return payload
}
