package healer

import (
	"encoding/json"
	"io"
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
	topicMetadatas       []*TopicMetadata
	simpleConsumers      []*SimpleConsumer

	mutex              sync.Locker
	assignmentStrategy AssignmentStrategy
}

func NewGroupConsumer(brokerList, topic, clientID, groupID string, sessionTimeout int, maxWaitTime int32, minBytes int32, maxBytes int32) (*GroupConsumer, error) {

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
		maxWaitTime:    maxWaitTime,
		minBytes:       minBytes,
		maxBytes:       maxBytes,

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
	c.topicMetadatas = metaDataResponse.TopicMetadatas
	glog.Infof("there is %d partitions in topic[%s]", len(c.topicMetadatas[0].PartitionMetadatas), c.topic)
	return nil
}

func (c *GroupConsumer) getCoordinator() error {
	// find coordinator
	coordinatorResponse, err := c.brokers.FindCoordinator(c.clientID, c.groupID)
	if err != nil {
		return err
	}

	coordinatorBroker, err := c.brokers.GetBroker(coordinatorResponse.Coordinator.nodeID)
	if err != nil {
		return err
	}
	glog.Infof("coordinator for group[%s]:%s", c.groupID, coordinatorBroker.address)
	c.coordinator = coordinatorBroker

	return nil
}

// join && set generationID&memberID
func (c *GroupConsumer) join() (*JoinGroupResponse, error) {
	glog.Info("try to join group")
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
		return nil, err
	}

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

//TODO need SyncGroupResponse returned?
func (c *GroupConsumer) sync() (*SyncGroupResponse, error) {
	glog.Info("try to sync group")
	var groupAssignment GroupAssignment
	if c.ifLeader {
		groupAssignment = c.assignmentStrategy.Assign(c.members, c.topicMetadatas)
	} else {
		groupAssignment = nil
	}
	glog.V(5).Infof("group assignment:%v", groupAssignment)

	syncGroupResponse, err := c.coordinator.requestSyncGroup(
		c.clientID, c.groupID, c.generationID, c.memberID, groupAssignment)

	if err != nil {
		return nil, err
	}

	c.parseGroupAssignments(syncGroupResponse.MemberAssignment)

	return syncGroupResponse, nil
}

func (c *GroupConsumer) parseGroupAssignments(memberAssignmentPayload []byte) error {
	memberAssignment, err := NewMemberAssignment(memberAssignmentPayload)
	if err != nil {
		return err
	}
	c.partitionAssignments = memberAssignment.PartitionAssignments
	c.simpleConsumers = make([]*SimpleConsumer, 0)

	for _, partitionAssignment := range c.partitionAssignments {
		for _, partitionID := range partitionAssignment.Partitions {
			simpleConsumer := &SimpleConsumer{}
			simpleConsumer.ClientID = c.clientID
			simpleConsumer.Brokers = c.brokers
			simpleConsumer.TopicName = partitionAssignment.Topic
			simpleConsumer.Partition = partitionID
			simpleConsumer.MaxWaitTime = c.maxWaitTime
			simpleConsumer.MaxBytes = c.maxBytes
			simpleConsumer.MinBytes = c.minBytes
			simpleConsumer.GroupID = c.groupID

			c.simpleConsumers = append(c.simpleConsumers, simpleConsumer)
		}
	}

	return nil
}

func (c *GroupConsumer) joinAndSync() {
	for {
		joinRes, err := c.join()
		if err != nil {
			glog.Infof("join error:%s", err)
			time.Sleep(time.Second * 1)
			continue
		} else {
			b, _ := json.Marshal(joinRes)
			glog.V(5).Infof("join response:%s", b)
		}

		syncRes, err := c.sync()
		if err != nil {
			glog.Infof("sync error:%s", err)
			time.Sleep(time.Second * 1)
			continue
		} else {
			b, _ := json.Marshal(syncRes)
			glog.V(5).Infof("sync response:%s", b)
		}

		return
	}
}

// connection is reset
func (c *GroupConsumer) resetConn() {
}

// TODO when need?
func (c *GroupConsumer) restart() {
}

func (c *GroupConsumer) heartbeat() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	glog.V(10).Infof("generationID:%d memberID:%s", c.generationID, c.memberID)
	r, err := c.coordinator.requestHeartbeat(c.clientID, c.groupID, c.generationID, c.memberID)
	if err != nil {
		glog.Errorf("failed to send heartbeat:%s", err)

		//The group is rebalancing, so a rejoin is needed
		if err == AllError[27] {
			glog.Info("rejoin because of rebalancing")
			c.joinAndSync()
		} else if err == io.EOF {
			glog.Info("rejoin because of EOF")
			c.joinAndSync()
		}
	}
	if r != nil {
		glog.V(8).Infof("heartbeat errorcode:%d", r.ErrorCode)
	}
}

func (c *GroupConsumer) Consume(fromBeginning bool, messages chan *FullMessage) (chan *FullMessage, error) {
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
	if messages == nil {
		messages = make(chan *FullMessage, 10)
	}
	for _, simpleConsumer := range c.simpleConsumers {
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
