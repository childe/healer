package healer

import (
	"encoding/binary"

	"github.com/golang/glog"
)

type PartitionBlock struct {
	Partition            int32
	current_leader_epoch int32
	FetchOffset          int64
	log_start_offset     int64
	MaxBytes             int32
}

type forgottenTopicsData struct {
	topic     string
	partition int32
}

type FetchRequest struct {
	*RequestHeader
	ReplicaId            int32
	MaxWaitTime          int32
	MinBytes             int32
	MaxBytes             int32
	isolationLevel       int8
	sessionId            int32
	sessionEpoch         int32
	Topics               map[string][]*PartitionBlock
	forgottenTopicsDatas []*forgottenTopicsData
}

// TODO all partitions should have the SAME maxbytes?
func NewFetchRequest(clientID string, maxWaitTime int32, minBytes int32) *FetchRequest {
	requestHeader := &RequestHeader{
		ApiKey:     API_FetchRequest,
		ApiVersion: 0,
		ClientId:   clientID,
	}

	topics := make(map[string][]*PartitionBlock)

	return &FetchRequest{
		RequestHeader: requestHeader,
		ReplicaId:     -1,
		MaxWaitTime:   maxWaitTime,
		MinBytes:      minBytes,
		Topics:        topics,
	}
}

func (fetchRequest *FetchRequest) addPartition(topic string, partitionID int32, fetchOffset int64, maxBytes int32) {
	if glog.V(10) {
		glog.Infof("fetch request %s[%d]:%d", topic, partitionID, fetchOffset)
	}
	partitionBlock := &PartitionBlock{
		Partition:   partitionID,
		FetchOffset: fetchOffset,
		MaxBytes:    maxBytes,
	}

	if value, ok := fetchRequest.Topics[topic]; ok {
		value = append(value, partitionBlock)
	} else {
		fetchRequest.Topics[topic] = []*PartitionBlock{partitionBlock}
	}
}

func (fetchRequest *FetchRequest) Encode(version uint16) []byte {
	requestLength := fetchRequest.RequestHeader.length() + 4 + 4 + 4
	requestLength += 4
	for topicname, partitionBlocks := range fetchRequest.Topics {
		requestLength += 2 + len(topicname)
		requestLength += 4 + len(partitionBlocks)*16
	}

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = fetchRequest.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.ReplicaId))
	offset += 4
	binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.MaxWaitTime))
	offset += 4
	binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.MinBytes))
	offset += 4

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(fetchRequest.Topics)))
	offset += 4
	for topicname, partitionBlocks := range fetchRequest.Topics {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(topicname)))
		offset += 2
		offset += copy(payload[offset:], topicname)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(partitionBlocks)))
		offset += 4
		for _, partitionBlock := range partitionBlocks {
			binary.BigEndian.PutUint32(payload[offset:], uint32(partitionBlock.Partition))
			offset += 4
			binary.BigEndian.PutUint64(payload[offset:], uint64(partitionBlock.FetchOffset))
			offset += 8
			binary.BigEndian.PutUint32(payload[offset:], uint32(partitionBlock.MaxBytes))
			offset += 4
		}
	}
	return payload
}
