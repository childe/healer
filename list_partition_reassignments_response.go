package healer

import (
	"encoding/binary"
	"fmt"
)

// ListPartitionReassignmentsResponse is a response from kafka to list partition reassignments
type ListPartitionReassignmentsResponse struct {
	CorrelationID  uint32                                 `json:"correlation_id"`
	ThrottleTimeMS int32                                  `json:"throttle_time_ms"`
	ErrorCode      int16                                  `json:"error_code"`
	ErrorMessage   *string                                `json:"error_message"`
	Topics         []listPartitionReassignmentsTopicBlock `json:"topics"`

	// TAG_BUFFER interface{}
}

type listPartitionReassignmentsTopicBlock struct {
	Name       string                                     `json:"name"`
	Partitions []listPartitionReassignmentsPartitionBlock `json:"partitions"`
	// TAG_BUFFER interface{}
}

func decodeToListPartitionReassignmentsTopicBlock(payload []byte) (r listPartitionReassignmentsTopicBlock, offset int) {
	var o int
	r.Name, o = compactString(payload[offset:])
	offset += o

	partitionCount, o := compactArrayLength(payload[offset:])
	offset += o
	if partitionCount > 0 {
		r.Partitions = make([]listPartitionReassignmentsPartitionBlock, partitionCount)
		for i := int32(0); i < partitionCount; i++ {
			r.Partitions[i], o = decodeToListPartitionReassignmentsPartitionBlock(payload[offset:])
			offset += o
		}
	}

	_, o = binary.Uvarint(payload[offset:]) // TAG_BUFFER
	offset += o

	return
}

type listPartitionReassignmentsPartitionBlock struct {
	Pid              int32   `json:"partition"`
	Replicas         []int32 `json:"replicas"`
	AddingReplicas   []int32 `json:"adding_replicas"`
	RemovingReplicas []int32 `json:"removing_replicas"`
	// TAG_BUFFER interface{}
}

func decodeToListPartitionReassignmentsPartitionBlock(payload []byte) (r listPartitionReassignmentsPartitionBlock, offset int) {
	r.Pid = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	replicaCount, o := compactArrayLength(payload[offset:])
	offset += o

	if replicaCount > 0 {
		r.Replicas = make([]int32, replicaCount)
		for i := int32(0); i < replicaCount; i++ {
			r.Replicas[i] = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
		}
	}

	addingReplicaCount, o := compactArrayLength(payload[offset:])
	offset += o
	if addingReplicaCount > 0 {
		r.AddingReplicas = make([]int32, addingReplicaCount)
		for i := int32(0); i < addingReplicaCount; i++ {
			r.AddingReplicas[i] = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
		}
	}

	removingReplicaCount, o := compactArrayLength(payload[offset:])
	offset += o
	if removingReplicaCount > 0 {
		r.RemovingReplicas = make([]int32, removingReplicaCount)
		for i := int32(0); i < removingReplicaCount; i++ {
			r.RemovingReplicas[i] = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
		}
	}
	_, o = binary.Uvarint(payload[offset:]) // TAG_BUFFER
	offset += o
	return
}

// FIXME: add error message too
func (r *ListPartitionReassignmentsResponse) Error() error {
	return getErrorFromErrorCode(r.ErrorCode)
}

// NewListPartitionReassignmentsResponse decode byte array to ListPartitionReassignmentsResponse instance
func NewListPartitionReassignmentsResponse(payload []byte, version uint16) (r *ListPartitionReassignmentsResponse, err error) {
	r = &ListPartitionReassignmentsResponse{}
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("ListPartitionReassignments response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ThrottleTimeMS = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	offset++ // I do not know what this byte means, always 0x01 , additonal byte after ErrorMessage and before Topics

	var o int
	r.ErrorMessage, o = compactNullableString(payload[offset:])
	offset += o

	topicCount, o := compactArrayLength(payload[offset:])
	offset += o

	if topicCount > 0 {
		r.Topics = make([]listPartitionReassignmentsTopicBlock, topicCount)

		for i := int32(0); i < topicCount; i++ {
			r.Topics[i], o = decodeToListPartitionReassignmentsTopicBlock(payload[offset:])
			offset += o
		}
	}

	_, o = binary.Uvarint(payload[offset:]) // TAG_BUFFER
	offset += o

	return r, err
}
