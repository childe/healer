package healer

import (
	"encoding/binary"
)

/*
This API answers the following questions:
- What topics exist?
- How many partitions does each topic have?
- Which broker is currently the leader for each partition?
- What is the host and port for each of these brokers?

This is the only request that can be addressed to any broker in the cluster.

Since there may be many topics the client can give an optional list of topic names in order to only return metadata for a subset of topics.

The metadata returned is at the partition level, but grouped together by topic for convenience and to avoid redundancy. For each partition the metadata contains the information for the leader as well as for all the replicas and the list of replicas that are currently in-sync.

Topic Metadata Request

	TopicMetadataRequest => [TopicName]
	  TopicName => string

Field			Description
TopicName		The topics to produce metadata for. If empty the request will yield metadata for all topics.
*/

type MetadataRequest struct {
	RequestHeader *RequestHeader
	Topic         []string
}

func (metadataRequest *MetadataRequest) Encode() []byte {
	requestHeaderLength := 8 + 2 + len(metadataRequest.RequestHeader.ClientId)
	requestLength := requestHeaderLength + 4
	for _, topic := range metadataRequest.Topic {
		requestLength += 2 + len(topic)
	}

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = metadataRequest.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(metadataRequest.Topic)))
	offset += 4

	for _, topicname := range metadataRequest.Topic {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(topicname)))
		offset += 2
		copy(payload[offset:], topicname)
		offset += len(topicname)
	}
	return payload
}
