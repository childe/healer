package healer

import (
	"encoding/binary"

	"github.com/golang/glog"
)

/*
The fetch API is used to fetch a chunk of one or more logs for some topic-partitions. Logically one specifies the topics, partitions, and starting offset at which to begin the fetch and gets back a chunk of messages. In general, the return messages will have offsets larger than or equal to the starting offset. However, with compressed messages, it's possible for the returned messages to have offsets smaller than the starting offset. The number of such messages is typically small and the caller is responsible for filtering out those messages.
Fetch requests follow a long poll model so they can be made to block for a period of time if sufficient data is not immediately available.
As an optimization the server is allowed to return a partial message at the end of the message set. Clients should handle this case.
One thing to note is that the fetch API requires specifying the partition to consume from. The question is how should a consumer know what partitions to consume from? In particular how can you balance the partitions over a set of consumers acting as a group so that each consumer gets a subset of partitions. We have done this assignment dynamically using zookeeper for the scala and java client. The downside of this approach is that it requires a fairly fat client and a zookeeper connection. We haven't yet created a Kafka API to allow this functionality to be moved to the server side and accessed more conveniently. A simple consumer client can be implemented by simply requiring that the partitions be specified in config, though this will not allow dynamic reassignment of partitions should that consumer fail. We hope to address this gap in the next major release.


FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
  ReplicaId => int32
  MaxWaitTime => int32
  MinBytes => int32
  TopicName => string
  Partition => int32
  FetchOffset => int64
  MaxBytes => int32

Field			Description
ReplicaId		The replica id indicates the node id of the replica initiating this request. Normal client consumers should always specify this as -1 as they have no node id. Other brokers set this to be their own node id. The value -2 is accepted to allow a non-broker to issue fetch requests as if it were a replica broker for debugging purposes.
MaxWaitTime		The max wait time is the maximum amount of time in milliseconds to block waiting if insufficient data is available at the time the request is issued.
MinBytes		This is the minimum number of bytes of messages that must be available to give a response. If the client sets this to 0 the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets. If this is set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs. By setting higher values in combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding).
TopicName		The name of the topic.
Partition		The id of the partition the fetch is for.
FetchOffset		The offset to begin this fetch from.
MaxBytes		The maximum bytes to include in the message set for this partition. This helps bound the size of the response.
*/

type PartitionBlock struct {
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
}

// version 0
type FetchRequest struct {
	*RequestHeader
	ReplicaId   int32
	MaxWaitTime int32
	MinBytes    int32
	Topics      map[string][]*PartitionBlock
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

func (fetchRequest *FetchRequest) Encode() []byte {
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
