package healer

import (
	"encoding/binary"
)

// AlterPartitionReassignmentsRequest is the request of AlterPartitionReassignmentsRequest
type AlterPartitionReassignmentsRequest struct {
	*RequestHeader
	TimeoutMs int32                               `json:"timeout_ms"`
	Topics    []*AlterPartitionReassignmentsTopic `json:"topics"`
}

// NewAlterPartitionReassignmentsRequest is used to create a new AlterPartitionReassignmentsRequest
func NewAlterPartitionReassignmentsRequest(timeoutMs int32) (r AlterPartitionReassignmentsRequest) {
	r.RequestHeader = &RequestHeader{
		APIKey:     API_AlterPartitionReassignments,
		APIVersion: 0,
	}
	r.TimeoutMs = timeoutMs
	return r
}

// AddAssignment is used to add a new assignment to AlterPartitionReassignmentsRequest
// It do not verify the assignment already exists or not
func (r *AlterPartitionReassignmentsRequest) AddAssignment(topic string, partitionID int32, replicas []int32) {
	for _, t := range r.Topics {
		if t.TopicName == topic {
			t.Partitions = append(t.Partitions, &AlterPartitionReassignmentsPartition{
				PartitionID: partitionID,
				Replicas:    replicas,
			})
			return
		}

	}
	r.Topics = append(r.Topics, &AlterPartitionReassignmentsTopic{
		TopicName: topic,
		Partitions: []*AlterPartitionReassignmentsPartition{
			{
				PartitionID: partitionID,
				Replicas:    replicas,
			},
		},
	})
}

func (r *AlterPartitionReassignmentsRequest) length(version uint16) (requestLength int) {
	requestLength = r.RequestHeader.length()
	requestLength += 4 // TimeoutMs
	requestLength += 4 // len(Topics)
	for _, topic := range r.Topics {
		requestLength += topic.length(version)
	}
	requestLength++ // TAG_BUFFER
	return requestLength
}

// Encode encodes AlterPartitionReassignmentsRequest to []byte
func (r *AlterPartitionReassignmentsRequest) Encode(version uint16) []byte {
	requestLength := r.length(version)
	payload := make([]byte, requestLength+4)

	offset := 4 // skip length field
	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(offset-4))
	}()

	offset += r.RequestHeader.Encode(payload[offset:])
	offset++ // TAG_BUFFER in header

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.TimeoutMs))
	offset += 4

	offset += binary.PutUvarint(payload[offset:], 1+uint64(len(r.Topics)))

	for _, topic := range r.Topics {
		offset += topic.encode(payload[offset:])
	}

	offset++ // TAG_BUFFER

	binary.BigEndian.PutUint32(payload[0:], uint32(offset-4))

	return payload[:offset]
}

// AlterPartitionReassignmentsTopic is the topic of AlterPartitionReassignmentsRequest
type AlterPartitionReassignmentsTopic struct {
	TopicName  string                                  `json:"topic_name"`
	Partitions []*AlterPartitionReassignmentsPartition `json:"partitions"`
	// TAG_BUFFER
}

func (r *AlterPartitionReassignmentsTopic) length(version uint16) (length int) {
	length = 2 // len(TopicName)
	length += len(r.TopicName)
	length += 4 // len(Partitions)
	for _, partition := range r.Partitions {
		length += partition.length(version)
	}
	length++ // TAG_BUFFER
	return
}

func (r *AlterPartitionReassignmentsTopic) encode(payload []byte) (offset int) {
	offset += binary.PutUvarint(payload[offset:], 1+uint64(len(r.TopicName)))
	offset += copy(payload[offset:], r.TopicName)

	offset += binary.PutUvarint(payload[offset:], 1+uint64(len(r.Partitions)))

	for _, partition := range r.Partitions {
		offset += partition.encode(payload[offset:])
	}

	offset++ // TAG_BUFFER
	return offset
}

// AlterPartitionReassignmentsPartition is the partition of AlterPartitionReassignmentsTopic
type AlterPartitionReassignmentsPartition struct {
	PartitionID int32   `json:"partition_id"`
	Replicas    []int32 `json:"replicas"`
	// TAG_BUFFER
}

func (r *AlterPartitionReassignmentsPartition) length(version uint16) (length int) {
	length = 4  // PartitionID
	length += 4 // len(Replicas)
	length += 4 * len(r.Replicas)
	length++ // TAG_BUFFER
	return
}

func (r *AlterPartitionReassignmentsPartition) encode(payload []byte) (offset int) {
	binary.BigEndian.PutUint32(payload[offset:], uint32(r.PartitionID))
	offset += 4

	offset += binary.PutUvarint(payload[offset:], 1+uint64(len(r.Replicas)))

	for _, replica := range r.Replicas {
		binary.BigEndian.PutUint32(payload[offset:], uint32(replica))
		offset += 4
	}

	offset++ // TAG_BUFFER
	return offset
}
