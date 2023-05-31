package healer

import (
	"encoding/binary"
	"fmt"

	"github.com/golang/glog"
)

// ListPartitionReassignmentsResponse is a response from kafka to list partition reassignments
type ListPartitionReassignmentsResponse struct {
	CorrelationID  uint32                                 `json:"correlation_id"`
	ThrottleTimeMS int32                                  `json:"throttle_time_ms"`
	ErrorCode      int16                                  `json:"error_code"`
	ErrorMessage   string                                 `json:"error_message"`
	Topics         []listPartitionReassignmentsTopicBlock `json:"topics"`

	// TAG_BUFFER interface{}
}

type listPartitionReassignmentsTopicBlock struct {
	Name       string                                     `json:"name"`
	Partitions []listPartitionReassignmentsPartitionBlock `json:"partitions"`
	// TAG_BUFFER interface{}
}

func decodeToListPartitionReassignmentsTopicBlock(payload []byte) (r listPartitionReassignmentsTopicBlock, offset int) {
	topicLength, o := binary.Uvarint(payload[offset:])
	topicLength--
	glog.Infof("topicName length: %d", topicLength)
	offset += o
	r.Name = string(payload[offset : offset+int(topicLength)])
	glog.Infof("topic name: %s", r.Name)
	offset += int(topicLength)

	partitionCount, o := binary.Uvarint(payload[offset:])
	partitionCount--
	offset += o
	r.Partitions = make([]listPartitionReassignmentsPartitionBlock, partitionCount)
	for i := uint64(0); i < partitionCount; i++ {
		r.Partitions[i], o = decodeToListPartitionReassignmentsPartitionBlock(payload[offset:])
		offset += o
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
}

func decodeToListPartitionReassignmentsPartitionBlock(payload []byte) (r listPartitionReassignmentsPartitionBlock, offset int) {
	r.Pid = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	replicaCount, o := binary.Uvarint(payload[offset:])
	replicaCount--
	offset += o

	if replicaCount > 0 {
		r.Replicas = make([]int32, replicaCount)
		for i := uint64(0); i < replicaCount; i++ {
			r.Replicas[i] = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
		}
	}

	addingReplicaCount, o := binary.Uvarint(payload[offset:])
	addingReplicaCount--
	offset += o
	if addingReplicaCount > 0 {
		r.AddingReplicas = make([]int32, addingReplicaCount)
		for i := uint64(0); i < addingReplicaCount; i++ {
			r.AddingReplicas[i] = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
		}
	}

	removingReplicaCount, o := binary.Uvarint(payload[offset:])
	removingReplicaCount--
	offset += o
	if removingReplicaCount > 0 {
		r.RemovingReplicas = make([]int32, removingReplicaCount)
		for i := uint64(0); i < removingReplicaCount; i++ {
			r.RemovingReplicas[i] = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
		}
	}
	return
}

// FIXME: add error message too
func (r ListPartitionReassignmentsResponse) Error() error {
	return getErrorFromErrorCode(r.ErrorCode)
}

// NewListPartitionReassignmentsResponse decode byte array to ListPartitionReassignmentsResponse instance
func NewListPartitionReassignmentsResponse(payload []byte, version uint16) (r ListPartitionReassignmentsResponse, err error) {
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("ListPartitionReassignments reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	glog.Infof("ListPartitionReassignmentsResponse.CorrelationID: %d", r.CorrelationID)
	offset += 4

	r.ThrottleTimeMS = int32(binary.BigEndian.Uint32(payload[offset:]))
	glog.Infof("ListPartitionReassignmentsResponse.ThrottleTimeMS: %d", r.ThrottleTimeMS)
	offset += 4

	r.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	glog.Infof("ListPartitionReassignmentsResponse.ErrorCode: %d", r.ErrorCode)
	offset += 2

	offset++ // I do not know what this byte means, always 0 , additonal byte before ErrorMessage

	length, o := binary.Uvarint(payload[offset:])
	length--
	glog.Infof("ListPartitionReassignmentsResponse.ErrorMessage length: %d %d", length, o)
	offset += o
	r.ErrorMessage = string(payload[offset : offset+int(length)])
	glog.Infof("ListPartitionReassignmentsResponse.ErrorMessage: %s", r.ErrorMessage)
	offset += int(length)

	topicCount, o := binary.Uvarint(payload[offset:])
	topicCount--
	glog.Infof("ListPartitionReassignmentsResponse.TopicCount: %d", topicCount)
	offset += o

	r.Topics = make([]listPartitionReassignmentsTopicBlock, topicCount)

	for i := uint64(0); i < topicCount; i++ {
		r.Topics[i], o = decodeToListPartitionReassignmentsTopicBlock(payload[offset:])
		offset += o
	}

	_, o = binary.Uvarint(payload[offset:]) // TAG_BUFFER
	offset += o

	return r, err
}
