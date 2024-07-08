package healer

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// GroupConsumer can join one group with other GroupConsumers with the same groupID
// and they consume messages from Kafka
// they will rebalance when new GroupConsumer joins or one leaves
type GroupConsumer struct {
	// TODO refresh metainfo in ticker
	brokers       *Brokers
	topic         string
	correlationID uint32

	config ConsumerConfig

	coordinator          *Broker
	generationID         int32
	memberID             string
	members              []Member        // maybe some members consume other topics , but they are in the same group
	topicMetadatas       []TopicMetadata // may contain some other topics which are consumed by other process with the same group
	ifLeader             bool
	joined               bool
	closed               bool
	closeChan            chan bool
	coordinatorAvailable bool
	topics               []string
	partitionAssignments []*PartitionAssignment
	simpleConsumers      []*SimpleConsumer

	messages chan *FullMessage

	mutex              sync.Locker
	wg                 sync.WaitGroup // wg is used to tell if all consumer has already stopped
	assignmentStrategy AssignmentStrategy

	restartLocker sync.Locker
}

// NewGroupConsumer cretae a new GroupConsumer
func NewGroupConsumer(topic string, config interface{}) (*GroupConsumer, error) {
	cfg, err := createConsumerConfig(config)
	if err != nil {
		return nil, err
	}
	if err := cfg.checkValid(); err != nil {
		return nil, err
	}
	if cfg.ClientID == "" {
		ts := strconv.Itoa(int(time.Now().UnixNano() / 1000000))
		hostname, err := os.Hostname()
		if err != nil {
			logger.Error(err, "could not get hostname used in clientID")
			cfg.ClientID = fmt.Sprintf("%s-%s", cfg.GroupID, ts)
		} else {
			cfg.ClientID = fmt.Sprintf("%s-%s-%s", cfg.GroupID, ts, hostname)
		}
	}

	brokerConfig := getBrokerConfigFromConsumerConfig(cfg)
	brokers, err := NewBrokersWithConfig(cfg.BootstrapServers, brokerConfig)
	if err != nil {
		return nil, err
	}

	c := &GroupConsumer{
		brokers:       brokers,
		topic:         topic,
		correlationID: 0,
		config:        cfg,

		mutex:              &sync.Mutex{},
		assignmentStrategy: &rangeAssignmentStrategy{},

		joined:               false,
		coordinatorAvailable: false,

		closeChan: make(chan bool, 1),

		restartLocker: &sync.Mutex{},
	}

	return c, nil
}

// request metadata and set partition metadat to group-consumer. only leader should request this
func (c *GroupConsumer) getTopicPartitionInfo() {
	// TODO if could not get meta, such as error 5:`There is no leader for this topic-partition as we are in the middle of a leadership election.`
	var (
		metaDataResponse MetadataResponse
		err              error
		_topics          = map[string]bool{}
	)
	for _, member := range c.members {
		protocolMetadata := NewProtocolMetadata(member.MemberMetadata)
		for _, topic := range protocolMetadata.Subscription {
			_topics[topic] = true
		}
	}

	c.topics = []string{}
	for t := range _topics {
		c.topics = append(c.topics, t)
	}
	for !c.closed {
		metaDataResponse, err = c.brokers.RequestMetaData(c.config.ClientID, c.topics)
		if err == nil {
			break
		} else {
			logger.Error(err, "failed to get metadata", "topics", c.topics)
			time.Sleep(1000 * time.Millisecond)
		}
	}

	b, _ := json.Marshal(metaDataResponse)
	logger.V(5).Info("got metadata", "topics", c.topics, "metadata", b)
	c.topicMetadatas = metaDataResponse.TopicMetadatas
}

// TODO getCoordinator may executed in dead loop and create too many connections to kafka clusters
// TODO put this to brokers?
func (c *GroupConsumer) getCoordinator() error {
	coordinatorResponse, err := c.brokers.FindCoordinator(c.config.ClientID, c.config.GroupID)
	if err != nil {
		return err
	}

	coordinatorBroker, err := c.brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
	if err != nil {
		return err
	}
	logger.Info("create coordinator", "groupId", c.config.GroupID, "address", coordinatorBroker.address)
	c.coordinator = coordinatorBroker

	return nil
}

func (c *GroupConsumer) parseGroupAssignments(memberAssignmentPayload []byte) error {
	memberAssignment, err := NewMemberAssignment(memberAssignmentPayload)
	if err != nil {
		return err
	}
	b, _ := json.Marshal(memberAssignment)
	logger.V(1).Info("parse memeber assignment", "assignment", b)
	c.partitionAssignments = memberAssignment.PartitionAssignments
	c.simpleConsumers = make([]*SimpleConsumer, 0)

	for _, partitionAssignment := range c.partitionAssignments {
		for _, partitionID := range partitionAssignment.Partitions {
			simpleConsumer := NewSimpleConsumerWithBrokers(partitionAssignment.Topic, partitionID, c.config, c.brokers)
			simpleConsumer.belongTO = c
			simpleConsumer.wg = &c.wg
			c.simpleConsumers = append(c.simpleConsumers, simpleConsumer)
		}
	}

	return nil
}

// join && set generationID&memberID
func (c *GroupConsumer) join() error {
	logger.Info("try to join group", "groupId", c.config.GroupID)
	var (
		protocolType = "consumer"
	)

	protocolMetadata := &ProtocolMetadata{
		Version:      0,
		Subscription: []string{c.topic},
		UserData:     nil,
	}

	gps := []*GroupProtocol{{"range", protocolMetadata.Encode()}}
	joinGroupResponse, err := c.coordinator.requestJoinGroup(
		c.config.ClientID, c.config.GroupID, int32(c.config.SessionTimeoutMS), c.memberID, protocolType, gps)

	if err != nil {
		logger.Error(err, "join group failed", "groupId", c.config.GroupID)

		if err == KafkaError(25) {
			c.memberID = ""
		}

		if err == io.EOF || err == KafkaError(15) || err == KafkaError(16) {
			c.coordinatorAvailable = false
		}

		if strings.Contains(err.Error(), "connection refused") {
			c.coordinatorAvailable = false
		}

		if strings.Contains(err.Error(), "network is unreachable") {
			c.coordinatorAvailable = false
		}
		if strings.Contains(err.Error(), "i/o timeout") {
			c.coordinatorAvailable = false
		}

		return err
	}

	c.generationID = joinGroupResponse.GenerationID
	c.memberID = joinGroupResponse.MemberID
	logger.Info("got new memberID after (re)join", "memberId", c.memberID)

	if joinGroupResponse.LeaderID == c.memberID {
		c.ifLeader = true
		c.members = joinGroupResponse.Members
	} else {
		c.ifLeader = false
	}
	return nil
}

func (c *GroupConsumer) sync() error {
	logger.Info("try to sync group", "groupId", c.config.GroupID)
	var groupAssignment GroupAssignment
	if c.ifLeader {
		c.getTopicPartitionInfo()
		groupAssignment = c.assignmentStrategy.Assign(c.members, c.topicMetadatas)
	} else {
		groupAssignment = nil
	}
	logger.V(5).Info("create group assignment", "assignment", groupAssignment)

	syncGroupResponse, err := c.coordinator.requestSyncGroup(
		c.config.ClientID, c.config.GroupID, c.generationID, c.memberID, groupAssignment)

	b, _ := json.Marshal(syncGroupResponse)
	logger.Info("sync returns", "response", b)

	if err != nil {
		logger.Error(err, "sync failed", "groupId", c.config.GroupID)
		if err == KafkaError(15) || err == KafkaError(16) {
			c.coordinatorAvailable = false
		}
		if err == KafkaError(25) {
			c.memberID = ""
		}
		return err
	}

	err = c.parseGroupAssignments(syncGroupResponse.MemberAssignment)
	if err != nil {
		return fmt.Errorf("failed to parse group assignment: %w", err)
	}

	return nil
}

func (c *GroupConsumer) joinAndSync() error {
	var err error
	for !c.closed {
		if !c.coordinatorAvailable {
			err = c.getCoordinator()
			if err != nil {
				logger.Error(err, "could not find coordinator")
				time.Sleep(time.Millisecond * time.Duration(c.config.RetryBackOffMS))
				continue
			}
		}
		c.coordinatorAvailable = true

		if c.closed {
			return nil
		}

		err = c.join()

		if c.closed {
			return nil
		}

		if err == nil {
			err = c.sync()
		}

		if err == nil {
			return nil
		}

		if err == KafkaError(22) || err == KafkaError(25) || err == KafkaError(27) {
			continue
		}
		if _, ok := err.(KafkaError); ok {
			if err.(KafkaError).IsRetriable() {
				time.Sleep(time.Millisecond * time.Duration(c.config.RetryBackOffMS))
				continue
			}
		}
		return err
	}
	return nil
}

func (c *GroupConsumer) heartbeat() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if !c.joined {
		return nil
	}

	logger.V(5).Info("heartbeat", "generationID", c.generationID, "memberID", c.memberID)
	_, err := c.coordinator.requestHeartbeat(c.config.ClientID, c.config.GroupID, c.generationID, c.memberID)
	return err
}

// CommitOffset commit offset to kafka server
func (c *GroupConsumer) CommitOffset() {
	for _, s := range c.simpleConsumers {
		s.CommitOffset()
	}
}

func (c *GroupConsumer) commitOffset(topic string, partitionID int32, offset int64) bool {
	if c.memberID == "" {
		logger.V(3).Info("do not commit offset because memberID is empty now", "topic", topic, "partitionID", partitionID, "offset", offset)
		return false
	}
	if offset < 0 {
		logger.V(3).Info("invalid commit offset", "offste", offset)
		return false
	}
	var apiVersion uint16
	if c.config.OffsetsStorage == 1 {
		apiVersion = 2
	} else {
		apiVersion = 0
	}
	offsetComimtReq := NewOffsetCommitRequest(apiVersion, c.config.ClientID, c.config.GroupID)
	offsetComimtReq.SetMemberID(c.memberID)
	offsetComimtReq.SetGenerationID(c.generationID)
	offsetComimtReq.SetRetentionTime(-1)
	offsetComimtReq.AddPartiton(topic, partitionID, offset, "")

	resp, err := c.coordinator.RequestAndGet(offsetComimtReq)
	if err == nil {
		err = resp.Error()
	}
	if err == nil {
		logger.V(3).Info("offset committed", "memberID", c.memberID, "generationID", c.generationID, "topic", topic, "partitionID", partitionID, "offset", offset)
		return true
	}
	logger.Error(err, "commit offset failed", "memberID", c.memberID, "generationID", c.generationID, "topic", topic, "partitionID", partitionID, "offset", offset)
	return false
}

func (c *GroupConsumer) restart() {
	// heartbeat and metadata changing could both cause restart. make sure they do not conflict
	c.restartLocker.Lock()

	c.stop()
	// stop heartbeat
	c.joined = false
	c.consumeWithoutHeartBeat(c.config.FromBeginning)

	c.restartLocker.Unlock()
}

func (c *GroupConsumer) stop() {
	logger.Info("stop group consumer", "GroupID", c.config.GroupID)
	if c.simpleConsumers != nil {
		logger.Info("stop simple consumers", "count", len(c.simpleConsumers))
		for _, simpleConsumer := range c.simpleConsumers {
			logger.Info("stop simple consumer", "topic", simpleConsumer.topic, "partitionID", simpleConsumer.partitionID)
			simpleConsumer.Stop()
		}
	}

	c.wg.Wait()
}

func (c *GroupConsumer) leave() {
	if !c.joined {
		logger.Info("not joined yet, leave directly")
		return
	}
	logger.Info("group consumer leaves", "memberID", c.memberID, "GroupID", c.config.GroupID)
	leaveReq := NewLeaveGroupRequest(c.config.ClientID, c.config.GroupID, c.memberID)
	_, err := c.coordinator.RequestAndGet(leaveReq)
	if err != nil {
		logger.Error(err, "leave group failed", "memberID", c.memberID)
		return
	}

	c.memberID = ""
	c.joined = false
}

// Close will wait all simple consumers stop and then return
// or timeout and return after 30s
func (c *GroupConsumer) Close() {
	c.AwaitClose(time.Second * 30)
}

// AwaitClose will wait all simple consumers stop and then return
// or timeout and return after some time
func (c *GroupConsumer) AwaitClose(timeout time.Duration) {
	defer c.brokers.Close()

	c.closed = true
	c.closeChan <- true

	c.stop()

	done := make(chan bool)
	go func() {
		c.wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		logger.Info("all simple consumers stopped. leave and return")
	case <-time.After(timeout):
		logger.Info("group consumer await timeout. return")
		return
	}

	c.leave()
}

// Consume will join group and then cosumes messages from kafka.
// it return a chan, and client could get messages from the chan
func (c *GroupConsumer) Consume(messages chan *FullMessage) (<-chan *FullMessage, error) {
	c.closed = false

	if messages == nil {
		messages = make(chan *FullMessage, 100)
	}
	c.messages = messages

	// go heartbeat
	ticker := time.NewTicker(time.Millisecond * time.Duration(c.config.SessionTimeoutMS) / 10)
	go func() {
		for range ticker.C {
			if c.closed {
				return
			}
			err := c.heartbeat()
			if err != nil {
				logger.Error(err, "failed to send heartbeat, restarts")
				c.restart()
			}
		}
	}()

	// go refresh topic metadata
	go func() {
		logger.Info("start metadata refresh goroutine", "interval", c.config.MetadataMaxAgeMS)
		var (
			ticker *time.Ticker = time.NewTicker(time.Millisecond * time.Duration(c.config.MetadataMaxAgeMS))
		)
		for range ticker.C {
			if c.closed {
				return
			}
			if !c.ifLeader {
				continue
			}

			logger.Info("refresh metadata (in goroutine)")

			metaDataResponse, err := c.brokers.RequestMetaData(c.config.ClientID, c.topics)
			if err != nil {
				logger.Error(err, "request metadata (in goroutine) failed")
				continue
			}

			if !ifTopicMetadatasSame(c.topicMetadatas, metaDataResponse.TopicMetadatas) {
				logger.Info("metadata changed, restart group consumer")
				c.topicMetadatas = metaDataResponse.TopicMetadatas
				c.restart()
			}
		}
	}()

	return c.consumeWithoutHeartBeat(c.config.FromBeginning)
}

func (c *GroupConsumer) consumeWithoutHeartBeat(fromBeginning bool) (chan *FullMessage, error) {

	/* if groupconsumer restarts,
	it should wait all simple consumers stoped */
	c.wg.Wait()

	var err error
	joinedChan := make(chan bool, 1)
	go func() {
		for !c.closed {
			err = c.joinAndSync()
			if err == nil {
				break
			} else {
				time.Sleep(time.Millisecond * time.Duration(c.config.RetryBackOffMS))
			}
		}
		joinedChan <- true
	}()

	select {
	case <-c.closeChan:
		return c.messages, nil
	case <-joinedChan:
	}

	c.joined = true

	// consume
	for _, simpleConsumer := range c.simpleConsumers {
		var offset int64
		if fromBeginning {
			offset = -2
		} else {
			offset = -1
		}

		c.wg.Add(1)

		go simpleConsumer.Consume(offset, c.messages)
	}

	return c.messages, nil
}

// return true if same
func ifTopicMetadatasSame(a []TopicMetadata, b []TopicMetadata) bool {
	if a == nil && b == nil {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i].TopicName != b[i].TopicName || a[i].TopicErrorCode != b[i].TopicErrorCode {
			return false
		}

		if len(a[i].PartitionMetadatas) != len(b[i].PartitionMetadatas) {
			return false
		}

		for j := range a[i].PartitionMetadatas {
			if a[i].PartitionMetadatas[j].PartitionID != b[i].PartitionMetadatas[j].PartitionID {
				return false
			}
			if a[i].PartitionMetadatas[j].PartitionErrorCode != b[i].PartitionMetadatas[j].PartitionErrorCode {
				return false
			}
			if a[i].PartitionMetadatas[j].Leader != b[i].PartitionMetadatas[j].Leader {
				return false
			}
		}
	}

	return true
}
