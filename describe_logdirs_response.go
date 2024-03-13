package healer

import (
	"encoding/binary"
	"fmt"
)

// DescribeLogDirsResponse is a response of DescribeLogDirsRequest
type DescribeLogDirsResponse struct {
	CoordinatorID  uint32                          `json:"-"`
	ThrottleTimeMS int32                           `json:"throttle_time_ms"`
	Results        []describeLogDirsResponseResult `json:"results"`
}

func (r DescribeLogDirsResponse) Error() error {
	for _, result := range r.Results {
		if result.ErrorCode != 0 {
			return KafkaError(result.ErrorCode)
		}
	}
	return nil
}

type describeLogDirsResponseResult struct {
	ErrorCode int16                          `json:"error_code"`
	LogDir    string                         `json:"log_dir"`
	Topics    []describeLogDirsResponseTopic `json:"topics"`
}

type describeLogDirsResponseTopic struct {
	TopicName  string                             `json:"topic"`
	Partitions []describeLogDirsResponsePartition `json:"partitions"`
}

func decodeToDescribeLogDirsResponseTopic(payload []byte, version uint16) (r describeLogDirsResponseTopic, offset int, err error) {
	l := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	r.TopicName = string(payload[offset : offset+l])
	offset += l

	numPartitions := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	if numPartitions == -1 {
		r.Partitions = nil
		return
	} else if numPartitions == 0 {
		r.Partitions = []describeLogDirsResponsePartition{}
		return
	} else if numPartitions < 0 {
		err = fmt.Errorf("describe_logdirs response numPartitions < 0: %d", numPartitions)
		return
	} else {
		r.Partitions = make([]describeLogDirsResponsePartition, numPartitions)
	}

	var o int
	for i := 0; i < numPartitions; i++ {
		r.Partitions[i], o = decodeToDescribeLogDirsResponsePartition(payload[offset:], version)
		offset += o
	}

	return
}

type describeLogDirsResponsePartition struct {
	PartitionID int32 `json:"partition_id"`
	Size        int64 `json:"size"`
	OffsetLag   int64 `json:"offset_lag"`
	IsFutureKey bool  `json:"is_future_key"`
}

func decodeToDescribeLogDirsResponsePartition(payload []byte, version uint16) (r describeLogDirsResponsePartition, offset int) {
	r.PartitionID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	r.Size = int64(binary.BigEndian.Uint64(payload[offset:]))
	offset += 8

	r.OffsetLag = int64(binary.BigEndian.Uint64(payload[offset:]))
	offset += 8

	r.IsFutureKey = payload[offset] != 0
	offset++

	return
}

// NewDescribeLogDirsResponse create a DescribeLogDirsResponse from the given payload
func NewDescribeLogDirsResponse(payload []byte, version uint16) (r DescribeLogDirsResponse, err error) {
	offset := 0

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("describe_logdirs response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CoordinatorID = binary.BigEndian.Uint32(payload)
	offset += 4

	r.ThrottleTimeMS = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	numResults := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	if numResults == -1 {
		r.Results = nil
		return r, nil
	} else if numResults == 0 {
		r.Results = []describeLogDirsResponseResult{}
		return r, nil
	} else if numResults < 0 {
		return r, fmt.Errorf("describe_logdirs response numResults < 0: %d", numResults)
	} else {
		r.Results = make([]describeLogDirsResponseResult, numResults)
	}

	for i := 0; i < numResults; i++ {
		r.Results[i].ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		l := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		r.Results[i].LogDir = string(payload[offset : offset+l])
		offset += l

		numTopics := int(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		if numTopics == -1 {
			r.Results[i].Topics = nil
			continue
		} else if numTopics == 0 {
			r.Results[i].Topics = []describeLogDirsResponseTopic{}
			continue
		} else if numTopics < 0 {
			return r, fmt.Errorf("describe_logdirs response numTopics < 0: %d", numTopics)
		} else {
			r.Results[i].Topics = make([]describeLogDirsResponseTopic, numTopics)
		}

		var o int
		for j := 0; j < numTopics; j++ {
			r.Results[i].Topics[j], o, err = decodeToDescribeLogDirsResponseTopic(payload[offset:], version)
			offset += o
			if err != nil {
				return
			}
		}
	}
	return
}
