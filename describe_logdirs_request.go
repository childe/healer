package healer

import "encoding/binary"

// DescribeLogDirsRequestTopic is a topic in DescribeLogDirsRequest
type DescribeLogDirsRequestTopic struct {
	TopicName  string
	Partitions []int32
}

// DescribeLogDirsRequest is a request of DescribeLogDirsRequest
type DescribeLogDirsRequest struct {
	*RequestHeader
	Topics []DescribeLogDirsRequestTopic
}

// NewDescribeLogDirsRequest returns a new DescribeLogDirsRequest
func NewDescribeLogDirsRequest(clientID string, topics []string) (r DescribeLogDirsRequest) {
	r.RequestHeader = &RequestHeader{
		APIKey:   API_DescribeLogDirs,
		ClientID: &clientID,
	}
	r.Topics = make([]DescribeLogDirsRequestTopic, len(topics))
	for i, topic := range topics {
		r.Topics[i].TopicName = topic
		r.Topics[i].Partitions = nil
	}
	return r
}

// AddTopicPartition add a topic and partition to DescribeLogDirsRequest
func (r *DescribeLogDirsRequest) AddTopicPartition(topic string, pid int32) {
	for i, t := range r.Topics {
		if t.TopicName == topic {
			for _, p := range t.Partitions {
				if p == pid {
					return
				}
			}
			r.Topics[i].Partitions = append(r.Topics[i].Partitions, pid)
			return
		}
	}
	r.Topics = append(r.Topics, DescribeLogDirsRequestTopic{
		TopicName: topic,
		Partitions: []int32{
			pid,
		},
	})
}

func (r *DescribeLogDirsRequest) length(version uint16) int {
	l := r.RequestHeader.length()
	l += 4
	for _, topic := range r.Topics {
		l += 2 + len(topic.TopicName)
		l += 4
		l += 4 * len(topic.Partitions)
	}
	return l
}

// Encode encode DescribeLogDirsRequest to []byte
func (r DescribeLogDirsRequest) Encode(version uint16) []byte {
	requestLength := r.length(version)

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += r.RequestHeader.Encode(payload[offset:])

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Topics)))
	offset += 4

	for _, topic := range r.Topics {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(topic.TopicName)))
		offset += 2

		offset += copy(payload[offset:], topic.TopicName)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(topic.Partitions)))
		offset += 4

		for _, partition := range topic.Partitions {
			binary.BigEndian.PutUint32(payload[offset:], uint32(partition))
			offset += 4
		}
	}

	return payload[:offset]
}
