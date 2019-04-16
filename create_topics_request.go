package healer

import (
	"encoding/binary"
	"errors"
)

// CreateTopicsRequest v0 is described in http://kafka.apache.org/protocol.html
// CreateTopics Request (Version: 0) => [create_topic_requests] timeout
//  create_topic_requests => topic num_partitions replication_factor [replica_assignment] [config_entries]
//    topic => STRING
//    num_partitions => INT32
//    replication_factor => INT16
//    replica_assignment => partition [replicas]
//      partition => INT32
//      replicas => INT32
//    config_entries => config_name config_value
//      config_name => STRING
//      config_value => NULLABLE_STRING
//  timeout => INT32
type CreateTopicsRequest struct {
	RequestHeader       *RequestHeader
	CreateTopicRequests []*CreateTopicRequest
	Timeout             int32
}

// Length returns the length of bytes returned by Encode func
func (r *CreateTopicsRequest) Length() int {
	l := r.RequestHeader.length()
	l += 4
	for _, r := range r.CreateTopicRequests {
		l += r.length()
	}
	l += 4
	return l
}

// Encode encodes CreateTopicsRequest to binary bytes
func (r *CreateTopicsRequest) Encode() []byte {
	requestLength := r.Length()

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = r.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.CreateTopicRequests)))
	offset += 4

	for _, r := range r.CreateTopicRequests {
		offset += r.encode(payload[offset:])
	}

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.Timeout))
	//offset += 4

	return payload
}

// CreateTopicRequest is sub struct in CreateTopicsRequest
type CreateTopicRequest struct {
	Topic              string
	NumPartitions      int32
	ReplicationFactor  int16
	ReplicaAssignments []*ReplicaAssignment
	ConfigEntries      []*ConfigEntry
}

func (r *CreateTopicRequest) length() int {
	l := 2 + len(r.Topic)
	l += 4 + 2

	l += 4
	for _, ra := range r.ReplicaAssignments {
		l += 4 // Partition
		l += 4 //len(ra.Replicas)
		l += 4 * len(ra.Replicas)
	}

	l += 4
	for _, c := range r.ConfigEntries {
		l += 2 + len(c.ConfigName) + 2 + len(c.ConfigValue)
	}
	return l
}

func (r *CreateTopicRequest) encode(payload []byte) (offset int) {
	binary.BigEndian.PutUint16(payload[offset:], uint16(len(r.Topic)))
	offset += 2
	offset += copy(payload[offset:], r.Topic)

	binary.BigEndian.PutUint32(payload[offset:], uint32(r.NumPartitions))
	offset += 4

	binary.BigEndian.PutUint16(payload[offset:], uint16(r.ReplicationFactor))
	offset += 2

	binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.ReplicaAssignments)))
	offset += 4

	for _, ra := range r.ReplicaAssignments {
		binary.BigEndian.PutUint32(payload[offset:], uint32(ra.Partition))
		offset += 4

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(ra.Replicas)))
		offset += 4
		for _, replica := range ra.Replicas {
			binary.BigEndian.PutUint32(payload[offset:], uint32(replica))
			offset += 4
		}
	}

	for _, c := range r.ConfigEntries {
		binary.BigEndian.PutUint16(payload[offset:], uint16(len(c.ConfigName)))
		offset += 2
		offset += copy(payload[offset:], c.ConfigName)

		binary.BigEndian.PutUint16(payload[offset:], uint16(len(c.ConfigValue)))
		offset += 2
		offset += copy(payload[offset:], c.ConfigValue)
	}

	return
}

// ReplicaAssignment is sub struct in CreateTopicRequest
type ReplicaAssignment struct {
	Partition int32
	Replicas  []int32
}

// ConfigEntry is sub struct in CreateTopicRequest
type ConfigEntry struct {
	ConfigName  string
	ConfigValue string
}

func NewCreateTopicsRequest(clientID string, timeout int32) *CreateTopicsRequest {
	requestHeader := &RequestHeader{
		ApiKey:     API_CreateTopics,
		ApiVersion: 0,
		ClientId:   clientID,
	}

	return &CreateTopicsRequest{
		RequestHeader:       requestHeader,
		CreateTopicRequests: []*CreateTopicRequest{},
	}
}

// API return CreateTopicsRequest api key
func (r *CreateTopicsRequest) API() uint16 {
	return r.RequestHeader.ApiKey
}

// SetCorrelationID set correlationID to the request
func (r *CreateTopicsRequest) SetCorrelationID(c uint32) {
	r.RequestHeader.CorrelationID = c
}

var (
	errorDumplicatedTopic = errors.New("topic has been added to the create_topics request already")
)

// AddTopic gives user easy way to fill CreateTopicsRequest. it set ReplicaAssignment and ConfigEntries as nil, user can set them by AddReplicaAssignment and ADDConfigEntry
func (r *CreateTopicsRequest) AddTopic(topic string, partitions int32, replicationFactor int16) error {
	for _, rr := range r.CreateTopicRequests {
		if rr.Topic == topic {
			return errorDumplicatedTopic
		}
	}

	createTopicRequest := &CreateTopicRequest{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
	}
	r.CreateTopicRequests = append(r.CreateTopicRequests, createTopicRequest)
	return nil
}

var (
	errorTopicNotFound = errors.New("topic not found in the CreateTopicRequests")
	errorPidOutOfRange = errors.New("partitionID in replica-assignment is out of range")
)

// AddReplicaAssignment add replicas of certain topic & pid to CreateTopicRequests. It returns errorTopicNotFound if topic has not beed added to the request;  it overwrite replicas if pid exists
func (r *CreateTopicsRequest) AddReplicaAssignment(topic string, pid int32, replicas []int32) error {
	var rr *CreateTopicRequest
	var found = false
	for _, rr = range r.CreateTopicRequests {
		if rr.Topic == topic {
			found = true
			break
		}
	}

	if !found {
		return errorTopicNotFound
	}

	found = false
	for _, ra := range rr.ReplicaAssignments {
		if ra.Partition >= pid {
			return errorPidOutOfRange
		}

		if ra.Partition == pid {
			found = true
			ra.Replicas = replicas
			return nil
		}
	}

	if !found {
		rr.ReplicaAssignments = append(rr.ReplicaAssignments, &ReplicaAssignment{
			Partition: pid,
			Replicas:  replicas,
		})
	}

	return nil
}
