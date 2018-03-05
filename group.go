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
	Topic      string
	Partitions []int32
}

type MemberAssignment struct {
	Version              int16
	PartitionAssignments []*PartitionAssignment
	UserData             []byte
}

func (memberAssignment *MemberAssignment) Length() int {
	length := 2 + 4
	for _, p := range memberAssignment.PartitionAssignments {
		length += 2 + len(p.Topic)
		length += 4 + len(p.Partitions)*4
	}
	length += 4 + len(memberAssignment.UserData)
	return length
}

func (memberAssignment *MemberAssignment) Encode() []byte {
	payload := make([]byte, memberAssignment.Length())
	offset := 0

	binary.BigEndian.PutUint16(payload[offset:], uint16(memberAssignment.Version))
	offset += 2

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(memberAssignment.PartitionAssignments)))
	offset += 4
	for _, p := range memberAssignment.PartitionAssignments {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(p.Topic)))
		offset += 2

		copy(payload[offset:], p.Topic)
		offset += len(p.Topic)

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(p.Partitions)))
		offset += 4

		for _, partitionID := range p.Partitions {
			binary.BigEndian.PutUint32(payload[offset:], uint32(partitionID))
			offset += 4
		}
	}
	copy(payload[offset:], memberAssignment.UserData)

	return payload
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
		r.PartitionAssignments[i] = &PartitionAssignment{}
		topicLength := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		r.PartitionAssignments[i].Topic = string(payload[offset : offset+topicLength])
		offset += topicLength

		count := int(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		r.PartitionAssignments[i].Partitions = make([]int32, count)
		for j := 0; j < count; j++ {
			p := int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			r.PartitionAssignments[i].Partitions[j] = p
		}
	}

	count = int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	r.UserData = make([]byte, count)
	copy(r.UserData, payload)

	return r, nil
}

type GroupAssignment []struct {
	MemberID         string
	MemberAssignment []byte
}

// ProtocolMetadata is used in join request/response
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

func NewProtocolMetadata(payload []byte) (*ProtocolMetadata, error) {
	var (
		err    error = nil
		p            = &ProtocolMetadata{}
		offset       = 0
	)
	p.Version = binary.BigEndian.Uint16(payload[offset:])
	offset += 2
	SubscriptionCount := binary.BigEndian.Uint32(payload[offset:])
	offset += 4
	p.Subscription = make([]string, SubscriptionCount)
	for i := range p.Subscription {
		l := int(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2
		p.Subscription[i] = string(payload[offset : offset+l])
		offset += l
	}
	l := int(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4
	p.UserData = make([]byte, l)
	copy(p.UserData, payload[offset:offset+l])

	return p, err
}
