package healer

import (
	"encoding/binary"
	"fmt"
)

// AlterPartitionReassignmentsRequest is the request of AlterPartitionReassignmentsRequest
type AlterPartitionReassignmentsRequest struct {
	*RequestHeader
	TimeoutMs int32                               `json:"timeout_ms"`
	Topics    []*AlterPartitionReassignmentsTopic `json:"topics"`

	TaggedFields TaggedFields `json:"tagged_fields"`
}

// NewAlterPartitionReassignmentsRequest is used to create a new AlterPartitionReassignmentsRequest
func NewAlterPartitionReassignmentsRequest(timeoutMs int32) (r AlterPartitionReassignmentsRequest) {
	r.RequestHeader = &RequestHeader{
		APIKey: API_AlterPartitionReassignments,
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
	requestLength += r.TaggedFields.length()
	return requestLength
}

// Encode encodes AlterPartitionReassignmentsRequest to []byte
func (r *AlterPartitionReassignmentsRequest) Encode(version uint16) (payload []byte) {
	payload = make([]byte, r.length(version)+4)

	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(len(payload)-4))
	}()

	offset := 4
	offset += r.RequestHeader.Encode(payload[offset:])

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.TimeoutMs))
	offset += 4

	offset += copy(payload[offset:], encodeCompactArrayLength(len(r.Topics)))

	for _, topic := range r.Topics {
		offset += topic.encode(payload[offset:])
	}

	offset += copy(payload[offset:], r.TaggedFields.Encode())

	return payload[:offset]
}

// AlterPartitionReassignmentsTopic is the topic of AlterPartitionReassignmentsRequest
type AlterPartitionReassignmentsTopic struct {
	TopicName  string                                  `json:"topic_name"`
	Partitions []*AlterPartitionReassignmentsPartition `json:"partitions"`

	TaggedFields TaggedFields `json:"tagged_fields"`
}

func (r *AlterPartitionReassignmentsTopic) length(version uint16) (length int) {
	length = 2 // len(TopicName)
	length += len(r.TopicName)
	length += 4 // len(Partitions)
	for _, partition := range r.Partitions {
		length += partition.length(version)
	}
	length += r.TaggedFields.length()
	return
}

func (r *AlterPartitionReassignmentsTopic) encode(payload []byte) (offset int) {
	offset = copy(payload, encodeCompactString(r.TopicName))
	offset += copy(payload[offset:], encodeCompactArrayLength(len(r.Partitions)))

	for _, partition := range r.Partitions {
		offset += partition.encode(payload[offset:])
	}

	offset += copy(payload[offset:], r.TaggedFields.Encode())
	return offset
}

// AlterPartitionReassignmentsPartition is the partition of AlterPartitionReassignmentsTopic
type AlterPartitionReassignmentsPartition struct {
	PartitionID int32   `json:"partition_id"`
	Replicas    []int32 `json:"replicas"`

	TaggedFields TaggedFields `json:"tagged_fields"`
}

func (r *AlterPartitionReassignmentsPartition) length(version uint16) (length int) {
	length = 4  // PartitionID
	length += 4 // len(Replicas)
	length += 4 * len(r.Replicas)
	length += r.TaggedFields.length()
	return
}

func (r *AlterPartitionReassignmentsPartition) encode(payload []byte) (offset int) {
	binary.BigEndian.PutUint32(payload[offset:], uint32(r.PartitionID))
	offset += 4

	offset += copy(payload[offset:], encodeCompactArrayLength(len(r.Replicas)))

	for _, replica := range r.Replicas {
		binary.BigEndian.PutUint32(payload[offset:], uint32(replica))
		offset += 4
	}

	offset += copy(payload[offset:], r.TaggedFields.Encode())
	return offset
}

// just for test
func DecodeAlterPartitionReassignmentsRequest(payload []byte, version uint16) (r AlterPartitionReassignmentsRequest, err error) {
	responseLength := binary.BigEndian.Uint32(payload)
	offset := 4
	if responseLength+4 != uint32(len(payload)) {
		return r, fmt.Errorf("AlterPartitionReassignmentsRequest length did not match: %d!=%d", responseLength+4, len(payload))
	}

	requestHeader, o := DecodeRequestHeader(payload[offset:], version)
	r.RequestHeader = &requestHeader
	offset += o

	r.TimeoutMs = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	numTopics, o := compactArrayLength(payload[offset:])
	offset += o

	if numTopics < 0 {
		r.Topics = nil
	} else {
		r.Topics = make([]*AlterPartitionReassignmentsTopic, numTopics)
		for i := int32(0); i < numTopics; i++ {
			topic := &AlterPartitionReassignmentsTopic{}
			topic.TopicName, o = compactString(payload[offset:])
			offset += o

			numPartitions, bytesRead := compactArrayLength(payload[offset:])
			offset += bytesRead

			if numPartitions < 0 {
				topic.Partitions = nil
			} else {
				topic.Partitions = make([]*AlterPartitionReassignmentsPartition, numPartitions)
				for j := int32(0); j < numPartitions; j++ {
					partition := &AlterPartitionReassignmentsPartition{}
					partition.PartitionID = int32(binary.BigEndian.Uint32(payload[offset:]))
					offset += 4

					numReplicas, bytesRead := compactArrayLength(payload[offset:])
					offset += bytesRead

					if numReplicas < 0 {
						partition.Replicas = nil
					} else {

						partition.Replicas = make([]int32, numReplicas)
						for k := int32(0); k < numReplicas; k++ {
							partition.Replicas[k] = int32(binary.BigEndian.Uint32(payload[offset:]))
							offset += 4
						}
					}
					partition.TaggedFields, o = DecodeTaggedFields(payload[offset:], version)
					offset += o

					topic.Partitions[j] = partition
				}
			}

			topic.TaggedFields, o = DecodeTaggedFields(payload[offset:], version)
			offset += o

			r.Topics[i] = topic
		}
	}

	r.TaggedFields, o = DecodeTaggedFields(payload[offset:], version)
	offset += o
	return
}
