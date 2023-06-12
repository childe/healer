package healer

import "encoding/binary"

// CreatePartitionsRequest holds the parameters of a create-partitions request.
type CreatePartitionsRequest struct {
	*RequestHeader
	Topics       []createPartitionsRequestTopicBlock `json:"topics"`
	TimeoutMS    int32                               `json:"timeout_ms"`
	ValidateOnly bool                                `json:"validate_only"`
}

type createPartitionsRequestTopicBlock struct {
	Name        string  `json:"name"`
	Count       int32   `json:"count"`
	Assignments []int32 `json:"assignments"`
}

func (r createPartitionsRequestTopicBlock) encode(payload []byte, version uint16) (offset int) {
	copy(payload, r.Name)
	offset += 2 + len(r.Name)

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.Count))
	offset += 4

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Assignments)))
	offset += 4

	for _, assignment := range r.Assignments {
		binary.BigEndian.PutUint32(payload[offset:], uint32(assignment))
		offset += 4
	}

	return offset
}

func (r CreatePartitionsRequest) length(version uint16) (length int) {
	length = r.RequestHeader.length()
	length += 4
	for _, topic := range r.Topics {
		length += 2 + len(topic.Name)        // name
		length += 4                          // count
		length += 4                          // assignments length
		length += 4 * len(topic.Assignments) // assignments
	}
	length += 4 // timeout_ms
	length++    // validate_only
	return
}

// Encode encodes CreatePartitionsRequest to []byte
func (r CreatePartitionsRequest) Encode(version uint16) []byte {
	requestLength := r.length(version)

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset += r.RequestHeader.Encode(payload[offset:])

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Topics)))
	offset += 4

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

	return payload[:offset]
}
