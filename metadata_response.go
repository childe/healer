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
	Brokers        []*BrokerInfo
	ControllerID   int32
	TopicMetadatas []*TopicMetadata
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
	Replicas           []int32
	Isr                []int32
}

func decodeToPartitionMetadataInfo(payload []byte, version uint16) (p PartitionMetadataInfo, offset int) {
	p.PartitionErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2
	p.PartitionID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	p.Leader = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
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
	return
}

// NewMetadataResponse decodes a byte slice into a MetadataResponse object.
func NewMetadataResponse(payload []byte, version uint16) (*MetadataResponse, error) {
	var (
		err              error
		metadataResponse = &MetadataResponse{}
		offset           = 0
	)

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("MetadataResponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	metadataResponse.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	// Encode Brokers
	brokersCount := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	metadataResponse.Brokers = make([]*BrokerInfo, brokersCount)
	for i := 0; i < brokersCount; i++ {
		b, o := decodeToBrokerInfo(payload[offset:], 0)
		metadataResponse.Brokers[i] = &b
		offset += o
	}

	// ControllerID
	metadataResponse.ControllerID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	// Encode TopicMetadatas
	topicMetadatasCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	metadataResponse.TopicMetadatas = make([]*TopicMetadata, topicMetadatasCount)
	for i := uint32(0); i < topicMetadatasCount; i++ {
		tm, o := decodeToTopicMetadata(payload[offset:], version)
		metadataResponse.TopicMetadatas[i] = &tm
		offset += o
	}
	//end encode TopicMetadatas

	// sort by TopicName & PartitionID
	sort.Slice(metadataResponse.TopicMetadatas, func(i, j int) bool {
		return metadataResponse.TopicMetadatas[i].TopicName < metadataResponse.TopicMetadatas[j].TopicName
	})
	for _, p := range metadataResponse.TopicMetadatas {
		sort.Slice(p.PartitionMetadatas, func(i, j int) bool {
			return p.PartitionMetadatas[i].PartitionID < p.PartitionMetadatas[j].PartitionID
		})
	}

	return metadataResponse, err
}
