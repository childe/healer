package healer

import "encoding/binary"

// CreatePartitionsRequest holds the parameters of a create-partitions request.
type CreatePartitionsRequest struct {
	*RequestHeader
	Topics       []createPartitionsRequestTopicBlock `json:"topics"`
	TimeoutMS    int32                               `json:"timeout_ms"`
	ValidateOnly bool                                `json:"validate_only"`
	// TAG_BUFFER
}

// NewCreatePartitionsRequest creates a new CreatePartitionsRequest.
func NewCreatePartitionsRequest(clientID string, timeout uint32, validateOnly bool) CreatePartitionsRequest {
	requestHeader := &RequestHeader{
		APIKey:   API_CreatePartitions,
		ClientID: clientID,
	}

	return CreatePartitionsRequest{
		RequestHeader: requestHeader,
		TimeoutMS:     int32(timeout),
		ValidateOnly:  validateOnly,
	}
}

type createPartitionsRequestTopicBlock struct {
	Name        string                                  `json:"name"`
	Count       int32                                   `json:"count"`
	Assignments createPartitionsRequestAssignmentsBlock `json:"assignments"`
	// TAG_BUFFER
}

type createPartitionsRequestAssignmentsBlock struct {
	BrokerIDs []int32 `json:"broker_ids"`
	// TAG_BUFFER
}

func (r createPartitionsRequestAssignmentsBlock) encode(payload []byte, version uint16) (offset int) {
	if version == 2 {
		offset += binary.PutUvarint(payload[offset:], 1+uint64(len(r.BrokerIDs)))
	} else if version == 0 {
		binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.BrokerIDs)))
		offset += 4
	}

	for _, assignment := range r.BrokerIDs {
		binary.BigEndian.PutUint32(payload[offset:], uint32(assignment))
		offset += 4
	}

	// TAG_BUFFER
	if version == 2 {
		offset += binary.PutUvarint(payload[offset:], 0)
	}
	return offset
}

func (r *createPartitionsRequestTopicBlock) encode(payload []byte, version uint16) (offset int) {
	if version == 2 {
		offset += binary.PutUvarint(payload[offset:], 1+uint64(len(r.Name)))
	} else if version == 0 {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.Name)))
		offset += 2
	}

	copy(payload[offset:], r.Name)
	offset += len(r.Name)

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.Count))
	offset += 4

	offset += r.Assignments.encode(payload[offset:], version)

	return offset
}

func (r *CreatePartitionsRequest) length(version uint16) (length int) {
	length = r.RequestHeader.length()
	length += 4
	for _, topic := range r.Topics {
		length += 2 + len(topic.Name)                  // name
		length += 4                                    // count
		length += 4                                    // assignments length
		length += 4 * len(topic.Assignments.BrokerIDs) // assignments
	}
	length += 4 // timeout_ms
	length++    // validate_only
	if version == 2 {
		length += 4 // TAG_BUFFER
	}
	return
}

// AddTopic adds a topic to the request.
func (r *CreatePartitionsRequest) AddTopic(topic string, count int32, assignments []int32) {
	r.Topics = append(r.Topics, createPartitionsRequestTopicBlock{
		Name:  topic,
		Count: count,
		Assignments: createPartitionsRequestAssignmentsBlock{
			BrokerIDs: assignments,
		},
	})
}

// Encode encodes CreatePartitionsRequest to []byte
func (r CreatePartitionsRequest) Encode(version uint16) []byte {
	requestLength := r.length(version)

	payload := make([]byte, requestLength+4)
	offset := 0
	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(offset-4))
	}()

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += r.RequestHeader.Encode(payload[offset:])

	// TAG_BUFFER
	if version == 2 {
		offset += binary.PutUvarint(payload[offset:], 0)
	}

	if version == 2 {
		offset += binary.PutUvarint(payload[offset:], 1+uint64(len(r.Topics)))
	} else if version == 0 {
		binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Topics)))
		offset += 4
	}

	for _, topic := range r.Topics {
		offset += topic.encode(payload[offset:], version)
	}

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.TimeoutMS))
	offset += 4

	if r.ValidateOnly {
		payload[offset] = 1
	} else {
		payload[offset] = 0
	}
	offset++

	// TAG_BUFFER
	if version == 2 {
		offset += binary.PutUvarint(payload[offset:], 0)
	}

	return payload[:offset]
}
