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

	coordinator  *Broker
	generationID int32
	memberID     string
	ifLeader     bool

	mutex sync.Locker
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

		mutex: &sync.Mutex{},
	}

	return c, nil
}

func (c *GroupConsumer) getCoordinator() error {
	// find coordinator
	coordinatorResponse, err := c.brokers.FindCoordinator(c.topic, c.groupID)
	if err != nil {
		return err
	}

	coordinatorBroker := c.brokers.GetBroker(coordinatorResponse.Coordinator.nodeID)
	glog.Info(coordinatorBroker.address)
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
	glog.Infof("%s", b)

	c.generationID = joinGroupResponse.GenerationID
	c.memberID = joinGroupResponse.MemberID
	return joinGroupResponse, nil
}

//The sync group request is used by the group leader to assign state (e.g. partition assignments)
//to all members of the current generation. All members send SyncGroup immediately after
//joining the group, but only the leader provides the group's assignment.
func (c *GroupConsumer) sync() (*SyncGroupResponse, error) {
	syncGroupResponse, err := c.coordinator.requestSyncGroup(c.clientID, c.groupID, c.generationID, c.memberID)
	if err != nil {
		return nil, err
	}

	b, _ := json.Marshal(syncGroupResponse)
	glog.Infof("%s", b)

	return syncGroupResponse, nil
}
func (c *GroupConsumer) joinAndSync() {
	c.mutex.Lock()
	c.join()
	syncGroupResponse, err := c.sync()

	b, _ := json.Marshal(syncGroupResponse)
	glog.Infof("%s", b)

	if err != nil {
		glog.Fatalf("could not sync group:%s", err)
	}
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
		glog.Info(r.ErrorCode)
	}
}

func (c *GroupConsumer) parseGroupAssignments(memberAssignment []byte) {
}

func (c *GroupConsumer) Consume() (chan *FullMessage, error) {
	err := c.getCoordinator()
	if err != nil {
		glog.Fatalf("could not find coordinator:%s", err)
	}

	c.joinAndSync()

	// go heartbeat
	glog.Info(c.sessionTimeout)
	ticker := time.NewTicker(time.Millisecond * time.Duration(c.sessionTimeout) / 10)
	go func() {
		for range ticker.C {
			c.heartbeat()
		}
	}()

	time.Sleep(time.Second * 3600)

	// consume
	return nil, nil
}
