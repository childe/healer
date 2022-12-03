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

Topics Metadata Request

	TopicsMetadataRequest => [TopicsName]
	  TopicsName => string

Field			Description
TopicsName		The topics to produce metadata for. If empty the request will yield metadata for all topics.
*/

type MetadataRequest struct {
	*RequestHeader
	Topics                 []string
	AllowAutoTopicCreation bool
}

func (r *MetadataRequest) length(version uint16) int {
	requestHeaderLength := r.RequestHeader.length()
	requestLength := requestHeaderLength + 4
	for _, topic := range r.Topics {
		requestLength += 2 + len(topic)
	}
	if version == 7 {
		requestLength += 1
	}
	return requestLength
}

func (metadataRequest *MetadataRequest) Encode(version uint16) []byte {
	requestLength := metadataRequest.length(version)

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += metadataRequest.RequestHeader.Encode(payload[offset:])

	if metadataRequest.Topics == nil {
		var i int32 = -1
		binary.BigEndian.PutUint32(payload[offset:], uint32(i))
	} else {
		binary.BigEndian.PutUint32(payload[offset:], uint32(len(metadataRequest.Topics)))
	}
	offset += 4

	for _, topicname := range metadataRequest.Topics {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(topicname)))
		offset += 2
		offset += copy(payload[offset:], topicname)
	}

	if version == 7 {
		if metadataRequest.AllowAutoTopicCreation {
			payload[offset] = 1
		} else {
			payload[offset] = 0
		}
		offset++
	}
	return payload
}

func NewMetadataRequest(clientID string, version uint16, topics []string) *MetadataRequest {
	r := &MetadataRequest{
		RequestHeader: &RequestHeader{
			APIKey:     API_MetadataRequest,
			APIVersion: version,
			ClientID:   clientID,
		},
		Topics: topics,
	}

	return r
}
