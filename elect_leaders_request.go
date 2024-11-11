package healer

import "encoding/binary"

type ElectLeadersRequest struct {
	*RequestHeader

	ElectionType int8              `json:"election_type"`
	Topics       []*TopicPartition `json:"topics"`
	TimeoutMS    int32             `json:"timeout.ms"`
}

type TopicPartition struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

// NewElectLeadersRequest returns a new ElectLeadersRequest
func NewElectLeadersRequest(timeoutMS int32) ElectLeadersRequest {
	return ElectLeadersRequest{
		RequestHeader: &RequestHeader{
			APIKey: API_ElectLeaders,
		},
		Topics:    make([]*TopicPartition, 0),
		TimeoutMS: timeoutMS,
	}
}

// Add adds a topic partition to the request, it does not check if the topic partition already exists
func (r *ElectLeadersRequest) Add(topic string, pid int32) {
	for _, t := range r.Topics {
		if t.Topic == topic {
			t.Partitions = append(t.Partitions, pid)
			return
		}
	}
	r.Topics = append(r.Topics, &TopicPartition{
		Topic:      topic,
		Partitions: []int32{pid},
	})
	return
}

func (r *TopicPartition) encode(payload []byte, version uint16) (offset int) {
	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.Topic)))
	offset += 2
	offset += copy(payload[offset:], r.Topic)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Partitions)))
	offset += 4
	for _, partition := range r.Partitions {
		binary.BigEndian.PutUint32(payload[offset:], uint32(partition))
		offset += 4
	}
	return
}

func (r *ElectLeadersRequest) length(version uint16) int {
	length := r.RequestHeader.length()

	length += 4 // timeout_ms
	length++    // election_type
	length += 4 // topics length
	for _, topic := range r.Topics {
		length += 2 + len(topic.Topic)      // topic
		length += 4                         // partitions length
		length += 4 * len(topic.Partitions) // partitions
	}

	return length
}

// Encode encodes a create partitions request into []byte
func (r *ElectLeadersRequest) Encode(version uint16) []byte {
	requestLength := r.length(version)
	payload := make([]byte, requestLength+4)
	offset := 0
	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(offset-4))
	}()

	offset = 4
	offset += r.RequestHeader.EncodeTo(payload[offset:])

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Topics)))
	offset += 4
	for _, topic := range r.Topics {
		offset += topic.encode(payload[offset:], version)
	}

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.TimeoutMS))
	offset += 4

	return payload[:offset]
}
