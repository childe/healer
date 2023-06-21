package healer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

var (
	errNoTopicsInMetadata = errors.New("no topic returned in metadata response")
)

// MetadataResponse holds all the parameters of metadata response, including the brokers and topics
type MetadataResponse struct {
	CorrelationID  uint32
	ThrottleTimeMs int32
	Brokers        []*BrokerInfo
	ClusterID      string
	ControllerID   int32
	TopicMetadatas []TopicMetadata
}

// Error returns the error abstracted from the error code, actually it always returns nil
func (r MetadataResponse) Error() error {
	for _, topic := range r.TopicMetadatas {
		if topic.TopicErrorCode != 0 {
			err := getErrorFromErrorCode(topic.TopicErrorCode)
			return fmt.Errorf("%s error: %w", topic.TopicName, err)
		}
		for _, p := range topic.PartitionMetadatas {
			if p.PartitionErrorCode != 0 {
				err := getErrorFromErrorCode(p.PartitionErrorCode)
				return fmt.Errorf("%s-%d error: %w", topic.TopicName, p.PartitionID, err)
			}
		}
	}
	return nil
}

// BrokerInfo holds all the parameters of broker info, which is used in metadata response
type BrokerInfo struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string
}

func decodeToBrokerInfo(payload []byte, version uint16) (b BrokerInfo, offset int) {
	b.NodeID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	l := int(int16(binary.BigEndian.Uint16(payload[offset:])))
	offset += 2
	b.Host = string(payload[offset : offset+l])
	offset += l
	b.Port = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	l = int(int16(binary.BigEndian.Uint16(payload[offset:])))
	offset += 2
	if l > 0 {
		b.Rack = string(payload[offset : offset+l])
		offset += l
	}
	return
}

// TopicMetadata holds all the parameters of topic metadata, which is used in metadata response
type TopicMetadata struct {
	TopicErrorCode     int16
	TopicName          string
	IsInternal         bool
	PartitionMetadatas []*PartitionMetadataInfo
}

func decodeToTopicMetadata(payload []byte, version uint16) (tm TopicMetadata, offset int) {
	tm.TopicErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	topicNameLength := int(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	tm.TopicName = string(payload[offset : offset+topicNameLength])
	offset += topicNameLength

	tm.IsInternal = payload[offset] != 0
	offset++

	partitionMetadataInfoCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	tm.PartitionMetadatas = make([]*PartitionMetadataInfo, partitionMetadataInfoCount)
	for j := uint32(0); j < partitionMetadataInfoCount; j++ {
		p, o := decodeToPartitionMetadataInfo(payload[offset:], version)
		tm.PartitionMetadatas[j] = &p
		offset += o
	}
	return
}

// PartitionMetadataInfo holds all the parameters of partition metadata info, which is used in metadata response
type PartitionMetadataInfo struct {
	PartitionErrorCode int16
	PartitionID        int32
	Leader             int32
	LeaderEpoch        int32
	Replicas           []int32
	Isr                []int32
	OfflineReplicas    []int32
}

func decodeToPartitionMetadataInfo(payload []byte, version uint16) (p PartitionMetadataInfo, offset int) {
	p.PartitionErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	p.PartitionID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	p.Leader = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	if version == 7 {
		p.LeaderEpoch = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
	}
	replicasCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	p.Replicas = make([]int32, replicasCount)
	for k := uint32(0); k < replicasCount; k++ {
		p.Replicas[k] = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
	}
	isrCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	p.Isr = make([]int32, isrCount)
	for k := uint32(0); k < isrCount; k++ {
		p.Isr[k] = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
	}

	if version == 7 {
		count := binary.BigEndian.Uint32(payload[offset:])
		offset += 4
		p.OfflineReplicas = make([]int32, count)
		for k := uint32(0); k < count; k++ {
			p.OfflineReplicas[k] = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
		}
	}
	return
}

// NewMetadataResponse decodes a byte slice into a MetadataResponse object.
func NewMetadataResponse(payload []byte, version uint16) (r MetadataResponse, err error) {
	var (
		offset = 0
	)

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("MetadataResponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	if version == 7 {
		r.ThrottleTimeMs = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
	}

	// Encode Brokers
	brokersCount := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	r.Brokers = make([]*BrokerInfo, brokersCount)
	for i := 0; i < brokersCount; i++ {
		b, o := decodeToBrokerInfo(payload[offset:], 0)
		r.Brokers[i] = &b
		offset += o
	}

	if version == 7 {
		l := int(binary.BigEndian.Uint16(payload[offset:]))
		r.ClusterID = string(payload[offset : offset+l])
		offset += 2 + l
	}
	// ControllerID
	r.ControllerID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	// Encode TopicMetadatas
	topicMetadatasCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	r.TopicMetadatas = make([]TopicMetadata, topicMetadatasCount)
	for i := uint32(0); i < topicMetadatasCount; i++ {
		tm, o := decodeToTopicMetadata(payload[offset:], version)
		r.TopicMetadatas[i] = tm
		offset += o
	}
	//end encode TopicMetadatas

	// sort by TopicName & PartitionID
	sort.Slice(r.TopicMetadatas, func(i, j int) bool {
		return r.TopicMetadatas[i].TopicName < r.TopicMetadatas[j].TopicName
	})
	for _, p := range r.TopicMetadatas {
		sort.Slice(p.PartitionMetadatas, func(i, j int) bool {
			return p.PartitionMetadatas[i].PartitionID < p.PartitionMetadatas[j].PartitionID
		})
	}

	return r, err
}
