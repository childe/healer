package healer

import (
	"encoding/binary"
)

// ListPartitionReassignmentsRequest is a request to kafka to list partition reassignments
type ListPartitionReassignmentsRequest struct {
	*RequestHeader
	TimeoutMS int32
	Topics    []struct {
		Name       string
		Partitions []int32
		// TaggedFields interface{}
	}
	// TaggedFields interface{}
}

// NewListPartitionReassignmentsRequest creates a new ListPartitionReassignmentsRequest
func NewListPartitionReassignmentsRequest(clientID string, timeoutMS int32) ListPartitionReassignmentsRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_ListPartitionReassignments,
		APIVersion: 0,
		ClientID:   clientID,
	}
	return ListPartitionReassignmentsRequest{
		RequestHeader: requestHeader,

		TimeoutMS: timeoutMS,
	}
}

// AddTP adds a topic/partition to the request
func (r *ListPartitionReassignmentsRequest) AddTP(topicName string, pid int32) {
	for i := range r.Topics {
		topic := &r.Topics[i]
		if topic.Name == topicName {
			for _, _pid := range topic.Partitions {
				if _pid == pid {
					return
				}
			}
			topic.Partitions = append(topic.Partitions, pid)
			return
		}
	}
	r.Topics = append(r.Topics, struct {
		Name       string
		Partitions []int32
	}{
		Name:       topicName,
		Partitions: []int32{pid},
	})
}

func (r ListPartitionReassignmentsRequest) length() int {
	requestLength := r.RequestHeader.length()
	requestLength += 4 // TimeoutMS
	requestLength += 4 // len(Topics)
	for _, topic := range r.Topics {
		requestLength += 2 + len(topic.Name) // len(TopicName)
		requestLength += 4                   // len(Partitions)
		for range topic.Partitions {
			requestLength += 4 // PartitionID
		}
		requestLength++ // TaggedFields
	}
	requestLength++ // TaggedFields
	return requestLength
}

// Encode encodes a ListPartitionReassignmentsRequest into a byte array.
func (r ListPartitionReassignmentsRequest) Encode(version uint16) []byte {
	requestLength := r.length()

	payload := make([]byte, requestLength+4)
	offset := 0
	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(offset-4))
	}()

	offset += 4

	offset += r.RequestHeader.Encode(payload[offset:])
	offset++ // TaggedFields

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.TimeoutMS))
	offset += 4

	if r.Topics == nil {
		offset += binary.PutUvarint(payload[offset:], 0)
	} else {
		offset += binary.PutUvarint(payload[offset:], uint64(1+len(r.Topics)))
	}

	for _, topic := range r.Topics {
		offset += binary.PutUvarint(payload[offset:], uint64(1+len(topic.Name)))
		offset += copy(payload[offset:], topic.Name)

		offset += binary.PutUvarint(payload[offset:], uint64(1+len(topic.Partitions)))

		for _, pid := range topic.Partitions {
			binary.BigEndian.PutUint32(payload[offset:], uint32(pid))
			offset += 4
		}
		offset++ // TaggedFields
	}
	offset++ // TaggedFields

	return payload[:offset]
}
