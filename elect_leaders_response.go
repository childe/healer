package healer

import (
	"encoding/binary"
	"fmt"
)

type ElectLeadersResponse struct {
	CorrelationID uint32 `json:"correlation_id"`

	ThrottleTimeMS         int32                    `json:"throttle_time_ms"`
	ReplicaElectionResults []*ReplicaElectionResult `json:"replica_election_results"`
}

func (r *ElectLeadersResponse) Error() error {
	for _, replicaElectionResult := range r.ReplicaElectionResults {
		for _, partitionResult := range replicaElectionResult.PartitionResults {
			if partitionResult.ErrorCode != 0 {
				err := KafkaError(partitionResult.ErrorCode)
				return fmt.Errorf("%s-%d elect leader error: %w. error message: %s", replicaElectionResult.Topic, partitionResult.PartitionID, err, partitionResult.ErrorMessage)
			}
		}
	}
	return nil
}

type ReplicaElectionResult struct {
	Topic            string             `json:"topic"`
	PartitionResults []*PartitionResult `json:"partition_result"`
}

func newReplicaElectionResult(payload []byte, offset int, version uint16) (r *ReplicaElectionResult, nextOffset int) {
	r = &ReplicaElectionResult{}
	length := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	r.Topic = string(payload[offset : offset+length])
	offset += length

	numPartitionResults := binary.BigEndian.Uint32(payload[offset:])
	offset += 4
	r.PartitionResults = make([]*PartitionResult, numPartitionResults)
	for i := range r.PartitionResults {
		r.PartitionResults[i], offset = newPartitionResult(payload, offset, version)
	}

	return r, offset
}

type PartitionResult struct {
	PartitionID  int32  `json:"partition_id"`
	ErrorCode    int16  `json:"error_code"`
	ErrorMessage string `json:"error_message"`
}

func newPartitionResult(payload []byte, offset int, version uint16) (r *PartitionResult, nextOffset int) {
	r = &PartitionResult{}
	r.PartitionID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	length := int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	if length > 0 {
		r.ErrorMessage = string(payload[offset : offset+int(length)])
		offset += int(length)
	}

	return r, offset
}

// NewElectLeadersResponse creates a new ElectLeadersResponse.
func NewElectLeadersResponse(payload []byte, version uint16) (r *ElectLeadersResponse, err error) {
	r = &ElectLeadersResponse{}

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("response length did not match: %d != %d", responseLength+4, len(payload))
	}
	offset := 4
	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.ThrottleTimeMS = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	numReplicaElectionResults := binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	r.ReplicaElectionResults = make([]*ReplicaElectionResult, numReplicaElectionResults)
	for i := range r.ReplicaElectionResults {
		r.ReplicaElectionResults[i], offset = newReplicaElectionResult(payload, offset, version)
	}

	return
}
