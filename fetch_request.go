package healer

import (
	"encoding/binary"
)

// PartitionBlock is the partition to fetch.
type PartitionBlock struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LogStartOffset     int64
	MaxBytes           int32
}

// FetchRequest holds all the parameters of fetch request
type FetchRequest struct {
	*RequestHeader
	ReplicaID            int32
	MaxWaitTime          int32
	MinBytes             int32
	MaxBytes             int32
	ISOLationLevel       int8
	SessionID            int32
	SessionEpoch         int32
	Topics               map[string][]*PartitionBlock
	ForgottenTopicsDatas map[string][]int32
}

// NewFetchRequest creates a new FetchRequest
func NewFetchRequest(clientID string, maxWaitTime int32, minBytes int32) *FetchRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_FetchRequest,
		APIVersion: 10,
		ClientID:   &clientID,
	}

	topics := make(map[string][]*PartitionBlock)

	return &FetchRequest{
		RequestHeader:        requestHeader,
		ReplicaID:            -1,
		MaxWaitTime:          maxWaitTime,
		MinBytes:             minBytes,
		Topics:               topics,
		ForgottenTopicsDatas: make(map[string][]int32),
	}
}

func (fetchRequest *FetchRequest) addPartition(topic string, partitionID int32, fetchOffset int64, maxBytes int32, currentLeaderEpoch int32) {
	logger.V(4).Info("add partition to fetch request", "topic", topic, "partitionID", partitionID, "fetchOffset", fetchOffset, "maxBytes", maxBytes)
	fetchRequest.MaxBytes += maxBytes
	partitionBlock := &PartitionBlock{
		Partition:          partitionID,
		CurrentLeaderEpoch: currentLeaderEpoch,
		FetchOffset:        fetchOffset,
		MaxBytes:           maxBytes,
	}

	if value, ok := fetchRequest.Topics[topic]; ok {
		fetchRequest.Topics[topic] = append(value, partitionBlock)
	} else {
		fetchRequest.Topics[topic] = []*PartitionBlock{partitionBlock}
	}
}

func (fetchRequest *FetchRequest) length(version uint16) int {
	length := 4 + fetchRequest.RequestHeader.length()
	length += 25 + 4 + 4
	for topicname := range fetchRequest.Topics {
		length += 2 + len(topicname) + 28
	}
	for topicname, partitionIDs := range fetchRequest.ForgottenTopicsDatas {
		length += 2 + len(topicname) + 4 + len(partitionIDs)*4
	}

	return length * 2
}

// Encode encodes request to []byte
func (fetchRequest *FetchRequest) Encode(version uint16) []byte {
	fetchRequest.RequestHeader.APIVersion = version

	requestLength := fetchRequest.length(version)
	payload := make([]byte, requestLength)
	offset := 4 // payload[:4] is requestLength, it will be filled at the end

	offset += fetchRequest.RequestHeader.EncodeTo(payload[offset:])

	binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.ReplicaID))
	offset += 4
	binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.MaxWaitTime))
	offset += 4
	binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.MinBytes))
	offset += 4

	if version >= 7 {
		binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.MaxBytes))
		offset += 4
		payload[offset] = byte(fetchRequest.ISOLationLevel)
		offset++
	}
	if version >= 7 {
		binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.SessionID))
		offset += 4
	}
	if version >= 7 {
		binary.BigEndian.PutUint32(payload[offset:], uint32(fetchRequest.SessionEpoch))
		offset += 4
	}

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
			if version >= 10 {
				binary.BigEndian.PutUint32(payload[offset:], uint32(partitionBlock.CurrentLeaderEpoch))
				offset += 4
			}
			binary.BigEndian.PutUint64(payload[offset:], uint64(partitionBlock.FetchOffset))
			offset += 8
			if version >= 7 {
				binary.BigEndian.PutUint64(payload[offset:], uint64(partitionBlock.LogStartOffset))
				offset += 8
			}
			binary.BigEndian.PutUint32(payload[offset:], uint32(partitionBlock.MaxBytes))
			offset += 4
		}
	}

	if version >= 7 {
		binary.BigEndian.PutUint32(payload[offset:], uint32(len(fetchRequest.ForgottenTopicsDatas)))
		offset += 4
		for topicName, partitions := range fetchRequest.ForgottenTopicsDatas {
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
	}

	binary.BigEndian.PutUint32(payload, uint32(offset-4))
	return payload[:offset]
}
