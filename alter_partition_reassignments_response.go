package healer

import (
	"encoding/binary"
	"fmt"
)

// AlterPartitionReassignmentsResponse is the response of AlterPartitionReassignmentsRequest
type AlterPartitionReassignmentsResponse struct {
	CorrelationID  uint32                                      `json:"correlation_id"`
	ThrottleTimeMs int32                                       `json:"throttle_time_ms"`
	ErrorCode      int16                                       `json:"error_code"`
	ErrorMsg       string                                      `json:"error_msg"`
	Responses      []*alterPartitionReassignmentsResponseTopic `json:"responses"`
	// TAG_BUFFER
}

func (r *AlterPartitionReassignmentsResponse) Error() error {
	if r.ErrorCode != 0 {
		return fmt.Errorf("%w: %s", getErrorFromErrorCode(r.ErrorCode), r.ErrorMsg)
	}
	for _, t := range r.Responses {
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				return fmt.Errorf("%w: %s", getErrorFromErrorCode(p.ErrorCode), p.ErrorMsg)
			}
		}
	}
	return nil
}

// alterPartitionReassignmentsResponseTopic is the response of AlterPartitionReassignmentsRequest
type alterPartitionReassignmentsResponseTopic struct {
	Name       string                                               `json:"name"`
	Partitions []*alterPartitionReassignmentsResponseTopicPartition `json:"partitions"`
	// TAG_BUFFER
}

type alterPartitionReassignmentsResponseTopicPartition struct {
	PartitionID int32  `json:"partition_id"`
	ErrorCode   int16  `json:"error_code"`
	ErrorMsg    string `json:"error_msg"`
	// TAG_BUFFER
}

func decodeToAlterPartitionReassignmentsResponseTopicPartition(payload []byte, version uint16) (p *alterPartitionReassignmentsResponseTopicPartition, length int) {
	p = &alterPartitionReassignmentsResponseTopicPartition{}
	offset := 0
	p.PartitionID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	p.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	errorMsg, o := compactNullableString(payload[offset:])
	offset += o
	p.ErrorMsg = errorMsg

	offset++ // TAG_BUFFER

	return p, offset
}

// NewAlterPartitionReassignmentsResponse create a new AlterPartitionReassignmentsResponse
func NewAlterPartitionReassignmentsResponse(payload []byte, version uint16) (*AlterPartitionReassignmentsResponse, error) {
	r := &AlterPartitionReassignmentsResponse{}
	var (
		offset int = 0
	)
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("AlterPartitionReassignments response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	offset++ // TAG_BUFFER

	r.ThrottleTimeMs = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	errorMsg, o := compactNullableString(payload[offset:])
	offset += o
	r.ErrorMsg = errorMsg

	responseCount, o := compactArrayLength(payload[offset:])
	offset += o
	if responseCount < 0 {
		r.Responses = nil
	}

	r.Responses = make([]*alterPartitionReassignmentsResponseTopic, responseCount)
	for i := 0; i < int(responseCount); i++ {
		topic := &alterPartitionReassignmentsResponseTopic{}
		r.Responses[i] = topic

		topic.Name, o = compactString(payload[offset:])
		offset += o

		partitionCount, o := compactArrayLength(payload[offset:])
		offset += o
		if partitionCount < 0 {
			topic.Partitions = nil
		}

		topic.Partitions = make([]*alterPartitionReassignmentsResponseTopicPartition, partitionCount)
		for j := 0; j < int(partitionCount); j++ {
			topic.Partitions[j], o = decodeToAlterPartitionReassignmentsResponseTopicPartition(payload[offset:], version)
			offset += o
		}

		offset++ // TAG_BUFFER
	}

	offset++ // TAG_BUFFER

	return r, nil
}
