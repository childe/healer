package healer

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/golang/glog"
)

type GroupConsumer struct {
	// TODO do not nedd one connection to each broker
	brokers        *Brokers
	topic          string
	correlationID  uint32
	clientID       string
	groupID        string
	sessionTimeout int
	maxWaitTime    int32
	maxBytes       int32
	minBytes       int32
	fromBeginning  bool

	coordinator          *Broker
	generationID         int32
	memberID             string
	members              []*Member
	ifLeader             bool
	partitionAssignments []*PartitionAssignment
	TopicMetadatas       []TopicMetadata
	simpleConsumers      []*SimpleConsumer

	mutex              sync.Locker
	assignmentStrategy AssignmentStrategy
}

func NewGroupConsumer(brokerList, topic, clientID, groupID string, sessionTimeout int) (*GroupConsumer, error) {

	brokers, err := NewBrokers(brokerList, clientID, 0, 0)
	if err != nil {
		return nil, err
	}
	c := &GroupConsumer{
		brokers:        brokers,
		topic:          topic,
		correlationID:  0,
		clientID:       clientID,
		groupID:        groupID,
		sessionTimeout: sessionTimeout,

		mutex:              &sync.Mutex{},
		assignmentStrategy: &RangeAssignmentStrategy{},
	}

	return c, nil
}

// request metadata and set partition metadat to group-consumer
// TODO maybe only leader should request this???
func (c *GroupConsumer) getTopicPartitionInfo() error {
	metaDataResponse, err := c.brokers.RequestMetaData(c.clientID, &c.topic)
	if err != nil {
		return err
	}

	b, _ := json.Marshal(metaDataResponse)
	glog.V(5).Infof("topic[%s] metadata:%s", c.topic, b)
	c.TopicMetadatas = metaDataResponse.TopicMetadatas
	glog.Infof("there is %d partitions in topic[%s]", len(c.TopicMetadatas[0].PartitionMetadatas), c.topic)
	return nil
}

func (c *GroupConsumer) getCoordinator() error {
	// find coordinator
	coordinatorResponse, err := c.brokers.FindCoordinator(c.clientID, c.groupID)
	if err != nil {
		return err
	}

	coordinatorBroker := c.brokers.GetBroker(coordinatorResponse.Coordinator.nodeID)
	glog.Infof("coordinator for group[%s]:%s", c.groupID, coordinatorBroker.address)
	c.coordinator = coordinatorBroker

	return nil
}

// join && set generationID&memberID
func (c *GroupConsumer) join() (*JoinGroupResponse, error) {
	// join
	var (
		protocolType string = "consumer"
		memberID     string = ""
	)

	protocolMetadata := &ProtocolMetadata{
		Version:      0,
		Subscription: []string{c.topic},
		UserData:     nil,
	}

	gps := []*GroupProtocol{&GroupProtocol{"range", protocolMetadata.Encode()}}
	joinGroupResponse, err := c.coordinator.requestJoinGroup(
		c.clientID, c.groupID, int32(c.sessionTimeout), memberID, protocolType,
		gps)
	if err != nil {
		glog.Fatalf("could not join group:%s", err)
	}
	b, _ := json.Marshal(joinGroupResponse)
	glog.V(5).Infof("joingroup response:%s", b)

	c.generationID = joinGroupResponse.GenerationID
	c.memberID = joinGroupResponse.MemberID
	glog.V(2).Infof("memberID now is %s", c.memberID)

	if joinGroupResponse.LeaderID == c.memberID {
		c.ifLeader = true
		c.members = joinGroupResponse.Members
	} else {
		c.ifLeader = false
	}
	return joinGroupResponse, nil
}

//The sync group request is used by the group leader to assign state (e.g. partition assignments)
//to all members of the current generation. All members send SyncGroup immediately after
//joining the group, but only the leader provides the group's assignment.
func (c *GroupConsumer) sync() (*SyncGroupResponse, error) {
	var groupAssignments []*GroupAssignment
	groupAssignments = make([]*GroupAssignment, 0)
	if c.ifLeader {
		groupAssignments = c.assignmentStrategy.assign(c.members, c.TopicMetadatas[0].PartitionMetadatas)
	} else {
		groupAssignments = nil
	}
	syncGroupResponse, err := c.coordinator.requestSyncGroup(c.clientID, c.groupID, c.generationID, c.memberID, groupAssignments)
	if err != nil {
		return nil, err
	}

	b, _ := json.Marshal(syncGroupResponse)
	glog.V(5).Infof("syncgroup response:%s", b)

	return syncGroupResponse, nil
}

func (c *GroupConsumer) joinAndSync() {
	c.mutex.Lock()
	// TODO: handle error
	c.join()
	c.sync()
	c.mutex.Unlock()
}

func (c *GroupConsumer) heartbeat() {
	r, err := c.coordinator.requestHeartbeat(c.clientID, c.groupID, c.generationID, c.memberID)
	if err != nil {
		glog.Errorf("failed to send heartbeat:%s", err)

		//The group is rebalancing, so a rejoin is needed
		if err == AllError[27] {
			c.joinAndSync()
		}
	}
	if r != nil {
		glog.V(8).Infof("heartbeat errorcode:%d", r.ErrorCode)
	}
}

func (c *GroupConsumer) parseGroupAssignments(memberAssignmentPayload []byte) error {
	memberAssignment, err := NewMemberAssignment(memberAssignmentPayload)
	if err != nil {
		return err
	}
	c.partitionAssignments = memberAssignment.PartitionAssignments
	c.simpleConsumers = make([]*SimpleConsumer, len(c.partitionAssignments))

	for i, partitionAssignment := range c.partitionAssignments {
		simpleConsumer := &SimpleConsumer{}
		simpleConsumer.ClientID = c.clientID
		simpleConsumer.Brokers = c.brokers
		simpleConsumer.TopicName = partitionAssignment.Topic
		simpleConsumer.Partition = partitionAssignment.Partition
		simpleConsumer.MaxWaitTime = c.maxWaitTime
		simpleConsumer.MaxBytes = c.maxBytes
		simpleConsumer.MinBytes = c.minBytes

		c.simpleConsumers[i] = simpleConsumer
	}

	return nil
}

func (c *GroupConsumer) Consume(fromBeginning bool) (chan *FullMessage, error) {
	err := c.getCoordinator()
	if err != nil {
		glog.Fatalf("could not find coordinator:%s", err)
	}

	c.getTopicPartitionInfo()
	c.joinAndSync()

	// go heartbeat
	ticker := time.NewTicker(time.Millisecond * time.Duration(c.sessionTimeout) / 10)
	go func() {
		for range ticker.C {
			c.heartbeat()
		}
	}()

	// consume
	var messages chan *FullMessage
	for _, simpleConsumer := range c.simpleConsumers {
		if err != nil {
			glog.Fatalf("could not get offset:%s", err)
		}
		var offset int64
		if fromBeginning {
			offset = -2
		} else {
			offset = -1
		}
		simpleConsumer.Consume(offset, messages)
	}

	return messages, nil
}
