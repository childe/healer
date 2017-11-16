package healer

import "github.com/golang/glog"

type GroupConsumer struct {
	// TODO do not nedd one connection to each broker
	brokers        *Brokers
	topic          string
	correlationID  uint32
	clientID       string
	groupID        string
	sessionTimeout int
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
	}

	return c, nil
}

func (c *GroupConsumer) Consume() (chan *FullMessage, error) {
	// find coordinator
	coordinatorResponse, err := c.brokers.FindCoordinator(c.topic, c.groupID)
	if err != nil {
		glog.Fatalf("could not get coordinator:%s", err)
	}
	// join
	coordinatorBroker := c.brokers.GetBroker(coordinatorResponse.Coordinator.nodeID)
	glog.Info(coordinatorBroker.address)

	var (
		protocolType string = "consumer"
		memberID     string = ""
	)
	joinGroupResponse, err := coordinatorBroker.requestJoinGroup(c.clientID, c.groupID, int32(c.sessionTimeout), memberID, protocolType)
	if err != nil {
		glog.Fatalf("could not join group:%s", err)
	}
	glog.Infof("%v", joinGroupResponse)

	// sync
	// go heartbeat
	// consume
	return nil, nil
}
