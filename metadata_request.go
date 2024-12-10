package healer

import (
	"encoding/binary"
)

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
	if version >= 4 {
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

	offset += metadataRequest.RequestHeader.EncodeTo(payload[offset:])

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

	if version >= 4 {
		if metadataRequest.AllowAutoTopicCreation {
			payload[offset] = 1
		} else {
			payload[offset] = 0
		}
		offset++
	}
	return payload
}

func NewMetadataRequest(clientID string, topics []string) *MetadataRequest {
	r := &MetadataRequest{
		RequestHeader: &RequestHeader{
			APIKey:   API_MetadataRequest,
			ClientID: &clientID,
		},
		Topics:                 topics,
		AllowAutoTopicCreation: true,
	}

	return r
}

// just for test
func DecodeMetadataRequest(payload []byte) (r MetadataRequest, err error) {
	offset := 0

	// request payload length
	binary.BigEndian.Uint32(payload)
	offset += 4

	// request header
	header, n := DecodeRequestHeader(payload[offset:])
	offset += n
	r.RequestHeader = &header

	// topics
	topicCount := binary.BigEndian.Uint32(payload[offset:])
	offset += 4
	r.Topics = make([]string, topicCount)
	for i := uint32(0); i < topicCount; i++ {
		topicLength := binary.BigEndian.Uint16(payload[offset:])
		offset += 2
		r.Topics[i] = string(payload[offset : offset+int(topicLength)])
		offset += int(topicLength)
	}

	if header.APIVersion >= 4 {
		r.AllowAutoTopicCreation = payload[offset] == 1
		offset++
	}

	return r, nil
}
