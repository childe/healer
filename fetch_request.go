package healer

import (
	"encoding/binary"

	"github.com/golang/glog"
)

type PartitionBlock struct {
	Partition            int32
	current_leader_epoch int32
	FetchOffset          int64
	logStartOffset       int64
	MaxBytes             int32
}

type FetchRequest struct {
	*RequestHeader
	ReplicaID            int32
	MaxWaitTime          int32
	MinBytes             int32
	MaxBytes             int32
	isolationLevel       int8
	sessionID            int32
	sessionEpoch         int32
	Topics               map[string][]*PartitionBlock
	forgottenTopicsDatas map[string][]int32
}

// TODO all partitions should have the SAME maxbytes?
func NewFetchRequest(clientID string, maxWaitTime int32, minBytes int32) *FetchRequest {
	requestHeader := &RequestHeader{
		ApiKey:     API_FetchRequest,
		ApiVersion: 10,
		ClientId:   clientID,
	}

	topics := make(map[string][]*PartitionBlock)

	return &FetchRequest{
		RequestHeader:        requestHeader,
		ReplicaID:            -1,
		MaxWaitTime:          maxWaitTime,
		MinBytes:             minBytes,
		Topics:               topics,
		forgottenTopicsDatas: make(map[string][]int32),
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

	// fetchRequest.forgottenTopicsDatas[topic] = make([]int32, 0)
}

// Encode encodes request to []byte
func (fetchRequest *FetchRequest) Encode(version uint16) []byte {
	fetchRequest.RequestHeader.ApiVersion = version

	// fix me
	requestLength := 1024
	payload := make([]byte, requestLength)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = fetchRequest.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.ReplicaID))
	offset += 4
	binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.MaxWaitTime))
	offset += 4
	binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.MinBytes))
	offset += 4
	binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.MaxBytes))
	offset += 4
	payload[offset] = byte(fetchRequest.isolationLevel)
	offset++
	binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.sessionID))
	offset += 4
	binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.sessionEpoch))
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
			binary.BigEndian.PutUint32(payload[offset:], uint32(partitionBlock.current_leader_epoch))
			offset += 4
			binary.BigEndian.PutUint64(payload[offset:], uint64(partitionBlock.FetchOffset))
			offset += 8
			binary.BigEndian.PutUint64(payload[offset:], uint64(partitionBlock.logStartOffset))
			offset += 8
			binary.BigEndian.PutUint32(payload[offset:], uint32(partitionBlock.MaxBytes))
			offset += 4
		}
	}

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(fetchRequest.forgottenTopicsDatas)))
	offset += 4
	for topicName, partitions := range fetchRequest.forgottenTopicsDatas {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(topicName)))
		offset += 2
		offset += copy(payload[offset:], topicName)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(partitions)))
		offset += 4
		for _, p := range partitions {
			binary.BigEndian.PutUint32(payload[offset:], uint32(p))
			offset += 4
		}
	}

	binary.BigEndian.PutUint32(payload, uint32(offset-4))
	return payload[:offset]
}
