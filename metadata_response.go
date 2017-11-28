package healer

import (
	"encoding/binary"
	"fmt"
)

/*
The response contains metadata for each partition, with partitions grouped together by topic. This metadata refers to brokers by their broker id. The brokers each have a host and port.

	MetadataResponse  [Broker][TopicMetadata]
	  Broker  NodeId Host Port  (any number of brokers may be returned)
		NodeId  int32
		Host  string
		Port  int32
	  TopicMetadata  TopicErrorCode TopicName [PartitionMetadata]
		TopicErrorCode  int16
	  PartitionMetadata  PartitionErrorCode PartitionId Leader Replicas Isr
		PartitionErrorCode  int16
		PartitionId  int32
		Leader  int32
		Replicas  [int32]
		Isr  [int32]

Field		Description
Leader		The node id for the kafka broker currently acting as leader for this partition. If no leader exists because we are in the middle of a leader election this id will be -1.
Replicas	The set of alive nodes that currently acts as slaves for the leader for this partition.
Isr			The set subset of the replicas that are "caught up" to the leader
Broker		The node id, hostname, and port information for a kafka brokers
*/

type BrokerInfo struct {
	NodeId int32
	Host   string
	Port   int32
}

type PartitionMetadataInfo struct {
	PartitionErrorCode int16
	PartitionId        uint32
	Leader             int32
	Replicas           []int32
	Isr                []int32
}

type TopicMetadata struct {
	TopicErrorCode     int16
	TopicName          string
	PartitionMetadatas []*PartitionMetadataInfo
}

type MetadataResponse struct {
	CorrelationID  uint32
	Brokers        []BrokerInfo
	TopicMetadatas []TopicMetadata
}

func NewMetadataResponse(payload []byte) (*MetadataResponse, error) {
	metadataResponse := &MetadataResponse{}
	//TODO: actually we have judged if the lenght matches while reading data from connection
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("MetadataResponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	metadataResponse.CorrelationID = uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	// encode Brokers
	brokersCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	metadataResponse.Brokers = make([]BrokerInfo, brokersCount)
	for i := uint32(0); i < brokersCount; i++ {
		metadataResponse.Brokers[i].NodeId = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		HostLength := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		metadataResponse.Brokers[i].Host = string(payload[offset : offset+HostLength])
		offset += HostLength
		metadataResponse.Brokers[i].Port = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
	}
	// end encode Brokers

	// encode TopicMetadatas
	topicMetadatasCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	metadataResponse.TopicMetadatas = make([]TopicMetadata, topicMetadatasCount)
	for i := uint32(0); i < topicMetadatasCount; i++ {
		metadataResponse.TopicMetadatas[i].TopicErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		topicNameLength := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		metadataResponse.TopicMetadatas[i].TopicName = string(payload[offset : offset+topicNameLength])
		offset += topicNameLength

		partitionMetadataInfoCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		metadataResponse.TopicMetadatas[i].PartitionMetadatas = make([]*PartitionMetadataInfo, partitionMetadataInfoCount)
		for j := uint32(0); j < partitionMetadataInfoCount; j++ {
			metadataResponse.TopicMetadatas[i].PartitionMetadatas[j] = &PartitionMetadataInfo{}
			metadataResponse.TopicMetadatas[i].PartitionMetadatas[j].PartitionErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
			offset += 2
			metadataResponse.TopicMetadatas[i].PartitionMetadatas[j].PartitionId = binary.BigEndian.Uint32(payload[offset:])
			offset += 4
			metadataResponse.TopicMetadatas[i].PartitionMetadatas[j].Leader = int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			replicasCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			metadataResponse.TopicMetadatas[i].PartitionMetadatas[j].Replicas = make([]int32, replicasCount)
			for k := uint32(0); k < replicasCount; k++ {
				metadataResponse.TopicMetadatas[i].PartitionMetadatas[j].Replicas[k] = int32(binary.BigEndian.Uint32(payload[offset:]))
				offset += 4
			}
			isrCount := uint32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			metadataResponse.TopicMetadatas[i].PartitionMetadatas[j].Isr = make([]int32, isrCount)
			for k := uint32(0); k < isrCount; k++ {
				metadataResponse.TopicMetadatas[i].PartitionMetadatas[j].Isr[k] = int32(binary.BigEndian.Uint32(payload[offset:]))
				offset += 4
			}
		}
	}
	//end encode TopicMetadatas

	return metadataResponse, nil
}
