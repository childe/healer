package healer

type GroupConsumer struct {
	// TODO do not nedd one connection to each broker
	brokers       *Brokers
	topic         string
	correlationID int32
	clientID      string
	groupID       string
}

func NewGroupConsumer(brokerList, topic, clientID, groupID string) (*GroupConsumer, error) {

	brokers, err := NewBrokers(brokerList, clientID, 0, 0)
	if err != nil {
		return nil, err
	}
	c := &GroupConsumer{
		brokers:       brokers,
		topic:         topic,
		correlationID: 0,
		clientID:      clientID,
		groupID:       groupID,
	}

	return c, nil
}

func (c *GroupConsumer) Consume() (chan *FullMessage, error) {
	// join
	c.brokers.FindCoordinator(c.correlationID, c.topic, c.groupID)
	// sync
	// go heartbeat
	// consume
	return nil, nil
}
