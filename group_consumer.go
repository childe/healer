package healer

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
)

// GroupConsumer can join one group with other GroupConsumers with the same groupID
// and they consume messages from Kafka
// they will rebalance when new GroupConsumer joins or one leaves
type GroupConsumer struct {
	// TODO refresh metainfo in ticker
	brokers       *Brokers
	topic         string
	correlationID uint32

	config *ConsumerConfig

	coordinator          *Broker
	generationID         int32
	memberID             string
	members              []*Member        // maybe some members consume other topics , but they are in the same group
	topicMetadatas       []*TopicMetadata // may contain some other topics which are consumed by other process with the same group
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
func NewGroupConsumer(topic string, config *ConsumerConfig) (*GroupConsumer, error) {
	if err := config.checkValid(); err != nil {
		return nil, err
	}
	if config.ClientID == "" {
		ts := strconv.Itoa(int(time.Now().UnixNano() / 1000000))
		hostname, err := os.Hostname()
		if err != nil {
			glog.Infof("could not get hostname for clientID: %s", err)
			config.ClientID = fmt.Sprintf("%s-%s", config.GroupID, ts)
		} else {
			config.ClientID = fmt.Sprintf("%s-%s-%s", config.GroupID, ts, hostname)
		}
	}

	brokerConfig := getBrokerConfigFromConsumerConfig(config)
	brokers, err := NewBrokersWithConfig(config.BootstrapServers, brokerConfig)
	if err != nil {
		return nil, err
	}

	c := &GroupConsumer{
		brokers:       brokers,
		topic:         topic,
		correlationID: 0,
		config:        config,

		mutex:              &sync.Mutex{},
		assignmentStrategy: &RangeAssignmentStrategy{},

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
		metaDataResponse *MetadataResponse
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
	for {
		metaDataResponse, err = c.brokers.RequestMetaData(c.config.ClientID, c.topics)
		if err == nil {
			break
		} else {
			glog.Errorf("failed to get metadata of topic[%s]:%s", c.topics, err)
			time.Sleep(1000 * time.Millisecond)
		}
	}

	if glog.V(5) {
		b, _ := json.Marshal(metaDataResponse)
		glog.Infof("topics[%s] metadata :%s", c.topics, b)
	}
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
	glog.Infof("coordinator for group[%s]:%s", c.config.GroupID, coordinatorBroker.address)
	c.coordinator = coordinatorBroker

	return nil
}

func (c *GroupConsumer) parseGroupAssignments(memberAssignmentPayload []byte) error {
	memberAssignment, err := NewMemberAssignment(memberAssignmentPayload)
	if err != nil {
		return err
	}
	if glog.V(2) {
		b, _ := json.Marshal(memberAssignment)
		glog.Infof("memeber assignment:%s", b)
	}
	c.partitionAssignments = memberAssignment.PartitionAssignments
	c.simpleConsumers = make([]*SimpleConsumer, 0)

	for _, partitionAssignment := range c.partitionAssignments {
		for _, partitionID := range partitionAssignment.Partitions {
			simpleConsumer := NewSimpleConsumerWithBrokers(c.topic, partitionID, c.config, c.brokers)
			simpleConsumer.belongTO = c
			simpleConsumer.wg = &c.wg
			c.simpleConsumers = append(c.simpleConsumers, simpleConsumer)
		}
	}

	return nil
}

// join && set generationID&memberID
func (c *GroupConsumer) join() error {
	glog.Infof("try to join group %s", c.config.GroupID)
	var (
		protocolType = "consumer"
	)

	protocolMetadata := &ProtocolMetadata{
		Version:      0,
		Subscription: []string{c.topic},
		UserData:     nil,
	}

	gps := []*GroupProtocol{&GroupProtocol{"range", protocolMetadata.Encode()}}
	joinGroupResponse, err := c.coordinator.requestJoinGroup(
		c.config.ClientID, c.config.GroupID, int32(c.config.SessionTimeoutMS), c.memberID, protocolType, gps)

	if glog.V(2) {
		b, _ := json.Marshal(joinGroupResponse)
		glog.Infof("join response:%s", b)
	}

	if err != nil {
		glog.Infof("join %s error: %s", c.config.GroupID, err)

		if "*healer.Error" != reflect.TypeOf(err).String() {
			return err
		}

		if err == AllError[15] || err == AllError[16] {
			c.coordinatorAvailable = false
		}
		if err == AllError[25] {
			c.memberID = ""
		}
		return err
	}

	c.generationID = joinGroupResponse.GenerationID
	c.memberID = joinGroupResponse.MemberID
	glog.Infof("memberID now is %s", c.memberID)

	if joinGroupResponse.LeaderID == c.memberID {
		c.ifLeader = true
		c.members = joinGroupResponse.Members
	} else {
		c.ifLeader = false
	}
	return nil
}

func (c *GroupConsumer) sync() error {
	glog.Infof("try to sync group %s", c.config.GroupID)
	var groupAssignment GroupAssignment
	if c.ifLeader {
		c.getTopicPartitionInfo()
		groupAssignment = c.assignmentStrategy.Assign(c.members, c.topicMetadatas)
	} else {
		groupAssignment = nil
	}
	glog.V(5).Infof("group assignment: %v", groupAssignment)

	syncGroupResponse, err := c.coordinator.requestSyncGroup(
		c.config.ClientID, c.config.GroupID, c.generationID, c.memberID, groupAssignment)

	if glog.V(5) {
		b, _ := json.Marshal(syncGroupResponse)
		glog.Infof("sync response:%s", b)
	}

	if err != nil {
		glog.Infof("sync %s error: %s", c.config.GroupID, err)
		return err
	}

	err = c.parseGroupAssignments(syncGroupResponse.MemberAssignment)
	if err != nil {
		glog.Errorf("parse group assignments error: %s", err)
		return err
	}

	return nil
}

func (c *GroupConsumer) joinAndSync() error {
	var err error
	for !c.closed {
		if !c.coordinatorAvailable {
			err = c.getCoordinator()
			if err != nil {
				glog.Errorf("could not find coordinator: %s", err)
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

		if err == AllError[22] || err == AllError[25] || err == AllError[27] {
			continue
		}
		if "*healer.Error" == reflect.TypeOf(err).String() && err.(*Error).Retriable {
			time.Sleep(time.Millisecond * time.Duration(c.config.RetryBackOffMS))
			continue
		}
		return err
	}
	return nil
}

func (c *GroupConsumer) heartbeat() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.joined == false {
		return nil
	}

	glog.V(5).Infof("heartbeat generationID:%d memberID:%s", c.generationID, c.memberID)
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
		glog.V(5).Infof("do not commit offset [%s][%d]:%d because memberID is not available", topic, partitionID, offset)
		return false
	}
	if offset < 0 {
		glog.V(5).Infof("invalid offset %d", offset)
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

	payload, err := c.coordinator.Request(offsetComimtReq)
	if err == nil {
		_, err := NewOffsetCommitResponse(payload)
		if err == nil {
			glog.V(5).Infof("commit offset %s(%d) [%s][%d]:%d", c.memberID, c.generationID, topic, partitionID, offset)
			return true
		} else {
			glog.Errorf("commit offset %s(%d) [%s][%d]:%d error:%s", c.memberID, c.generationID, topic, partitionID, offset, err)
		}
	} else {
		glog.Errorf("commit offset %s(%d) [%s][%d]:%d error:%s", c.memberID, c.generationID, topic, partitionID, offset, err)
	}
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
	if c.simpleConsumers != nil {
		for _, simpleConsumer := range c.simpleConsumers {
			simpleConsumer.Stop()
		}
	}

	c.wg.Wait()
}

func (c *GroupConsumer) leave() {
	if !c.joined {
		glog.Info("not joined yet, leave directly")
		return
	}
	glog.Infof("leave %s from %s", c.memberID, c.config.GroupID)
	leaveReq := NewLeaveGroupRequest(c.config.ClientID, c.config.GroupID, c.memberID)
	payload, err := c.coordinator.Request(leaveReq)
	if err != nil {
		glog.Errorf("member %s could not leave group:%s", c.memberID, err)
		return
	}

	_, err = NewLeaveGroupResponse(payload)
	if err != nil {
		glog.Errorf("member %s could not leave group:%s", c.memberID, err)
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
		glog.Info("all simple consumers stopped. leave and return")
	case <-time.After(timeout):
		glog.Info("group consumer await timeout. return")
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
				glog.Errorf("failed to send heartbeat, restart: %s", err)
				c.restart()
			}
		}
	}()

	// go refresh topic metadata
	go func() {
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

			metaDataResponse, err := c.brokers.RequestMetaData(c.config.ClientID, c.topics)
			if err != nil {
				glog.Errorf("request metadata (in goroutine) error: %s", err)
				continue
			}

			if glog.V(5) {
				b, _ := json.Marshal(metaDataResponse)
				glog.Infof("topics[%s] metadata: %s", c.topics, b)
			}
			if !ifTopicMetadatasSame(c.topicMetadatas, metaDataResponse.TopicMetadatas) {
				glog.Info("metadata changed, restart")
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
func ifTopicMetadatasSame(a []*TopicMetadata, b []*TopicMetadata) bool {
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
