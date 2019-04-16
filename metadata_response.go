package healer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
)

var (
	zeroTopicMetadata = errors.New("no topic returned in metadata response")
)

type MetadataResponse struct {
	CorrelationID  uint32
	Brokers        []*BrokerInfo
	ControllerID   int32
	TopicMetadatas []*TopicMetadata
}

type BrokerInfo struct {
	NodeId int32
	Host   string
	Port   int32
	Rack   string
}

type TopicMetadata struct {
	TopicErrorCode     int16
	TopicName          string
	IsInternal         bool
	PartitionMetadatas []*PartitionMetadataInfo
}

type PartitionMetadataInfo struct {
	PartitionErrorCode int16
	PartitionID        int32
	Leader             int32
	Replicas           []int32
	Isr                []int32
}

func NewMetadataResponse(payload []byte) (*MetadataResponse, error) {
	var (
		err              error
		metadataResponse = &MetadataResponse{}
		offset           = 0
		l                int
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
		b := &BrokerInfo{}
		metadataResponse.Brokers[i] = b

		b.NodeId = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		l = int(int16(binary.BigEndian.Uint16(payload[offset:])))
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
	}

	// ControllerID
	metadataResponse.ControllerID = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	// Encode TopicMetadatas
	topicMetadatasCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	metadataResponse.TopicMetadatas = make([]*TopicMetadata, topicMetadatasCount)
	for i := uint32(0); i < topicMetadatasCount; i++ {
		tm := &TopicMetadata{}
		metadataResponse.TopicMetadatas[i] = tm

		tm.TopicErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
		if err == nil && tm.TopicErrorCode != 0 {
			err = getErrorFromErrorCode(tm.TopicErrorCode)
			if err == AllError[9] {
				err = nil
			}
		}
		offset += 2
		topicNameLength := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		tm.TopicName = string(payload[offset : offset+topicNameLength])
		offset += topicNameLength

		tm.IsInternal = payload[offset] != 0
		offset += 1

		partitionMetadataInfoCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		tm.PartitionMetadatas = make([]*PartitionMetadataInfo, partitionMetadataInfoCount)
		for j := uint32(0); j < partitionMetadataInfoCount; j++ {
			tm.PartitionMetadatas[j] = &PartitionMetadataInfo{}
			partitionErrorCode := int16(binary.BigEndian.Uint16(payload[offset:]))
			tm.PartitionMetadatas[j].PartitionErrorCode = partitionErrorCode
			if err == nil && partitionErrorCode != 0 {
				err = getErrorFromErrorCode(partitionErrorCode)
				if err == AllError[9] {
					err = nil
				}
			}
			offset += 2
			tm.PartitionMetadatas[j].PartitionID = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			tm.PartitionMetadatas[j].Leader = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			replicasCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			tm.PartitionMetadatas[j].Replicas = make([]int32, replicasCount)
			for k := uint32(0); k < replicasCount; k++ {
				tm.PartitionMetadatas[j].Replicas[k] = int32(binary.BigEndian.Uint32(payload[offset:]))
				offset += 4
			}
			isrCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			tm.PartitionMetadatas[j].Isr = make([]int32, isrCount)
			for k := uint32(0); k < isrCount; k++ {
				tm.PartitionMetadatas[j].Isr[k] = int32(binary.BigEndian.Uint32(payload[offset:]))
				offset += 4
			}
		}
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
