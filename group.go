package healer

import "encoding/binary"

/*--- --- --- --- --- --- --- ---
Consumer groups: Below we define the embedded protocol used by consumer groups.
We recommend all consumer implementations follow this format so that
tooling will work correctly across all clients.
ProtocolType => "consumer"

ProtocolName => AssignmentStrategy
  AssignmentStrategy => string

ProtocolMetadata => Version Subscription UserData
  Version => int16
  Subscription => [Topic]
    Topic => string
  UserData => bytes
The UserData field can be used by custom partition assignment strategies.
For example, in a sticky partitioning implementation, this field can contain
the assignment from the previous generation. In a resource-based assignment strategy,
it could include the number of cpus on the machine hosting each consumer instance.

Kafka Connect uses the "connect" protocol type and its protocol details
are internal to the Connect implementation.

Consumer Groups: The format of the MemberAssignment field for consumer groups is included below:
MemberAssignment => Version PartitionAssignment
  Version => int16
  PartitionAssignment => [Topic [Partition]]
    Topic => string
    Partition => int32
  UserData => bytes
--- --- --- --- --- --- --- ---*/

type PartitionAssignment struct {
	Topic     string
	Partition int32
}

type MemberAssignment struct {
	Version              int16
	PartitionAssignments []*PartitionAssignment
	UserData             []byte
}

func NewMemberAssignment(payload []byte) (*MemberAssignment, error) {
	r := &MemberAssignment{}
	offset := 0
	count := 0

	r.Version = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	count = int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	r.PartitionAssignments = make([]*PartitionAssignment, count)

	for i := 0; i < count; i++ {
		topicLength := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		topic := string(payload[offset : offset+topicLength])
		offset += topicLength
		partition := int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4

		r.PartitionAssignments[i] = &PartitionAssignment{
			Topic:     topic,
			Partition: partition,
		}
	}

	return r, nil
}

type GroupAssignment struct {
	MemberID         string
	MemberAssignment []byte
}

// ProtocolMetadata is used in join request
type ProtocolMetadata struct {
	Version      uint16
	Subscription []string
	UserData     []byte
}

func (m *ProtocolMetadata) Length() int {
	length := 2 + 4
	for _, subscription := range m.Subscription {
		length += 2
		length += len(subscription)
	}
	length += 4 + len(m.UserData)
	return length
}

func (m *ProtocolMetadata) Encode() []byte {

	payload := make([]byte, m.Length())
	offset := 0
	binary.BigEndian.PutUint16(payload[offset:], m.Version)
	offset += 2

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(m.Subscription)))
	offset += 4

	for _, subscription := range m.Subscription {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(subscription)))
		offset += 2
		copy(payload[offset:], subscription)
		offset += len(subscription)
	}
	binary.BigEndian.PutUint32(payload[offset:], uint32(len(m.UserData)))
	offset += 4
	copy(payload[offset:], m.UserData)

	return payload
}
