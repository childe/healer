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
	topics               []string
	fromBeginning        bool
	partitionAssignments []*PartitionAssignment
	simpleConsumers      []*SimpleConsumer

	messages chan *FullMessage

	mutex              sync.Locker
	wg                 sync.WaitGroup // wg is used to tell if all consumer has already stopped
	assignmentStrategy AssignmentStrategy
}

func NewGroupConsumer(topic string, config *ConsumerConfig) (*GroupConsumer, error) {
	var clientID string
	if config.ClientID == "" {
		clientID = config.GroupID
	}
	ts := strconv.Itoa(int(time.Now().UnixNano() / 1000000))
	hostname, err := os.Hostname()
	if err != nil {
		glog.Infof("could not get hostname for clientID:%s", err)
		clientID = fmt.Sprintf("%s-%s", clientID, ts)
	} else {
		clientID = fmt.Sprintf("%s-%s-%s", clientID, ts, hostname)
	}
	config.ClientID = clientID

	brokerConfig := getBrokerConfigFromConsumerConfig(config)

	brokers, err := NewBrokers(config.BootstrapServers, config.ClientID, brokerConfig)
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

		joined: false,
	}

	return c, nil
}

// request metadata and set partition metadat to group-consumer. only leader should request this
func (c *GroupConsumer) getTopicPartitionInfo() {
	// TODO if could not get meta, such as error 5:`There is no leader for this topic-partition as we are in the middle of a leadership election.`
	var (
		metaDataResponse *MetadataResponse
		err              error
		_topics          map[string]bool = map[string]bool{}
	)
	for _, member := range c.members {
		protocolMetadata := NewProtocolMetadata(member.MemberMetadata)
		for _, topic := range protocolMetadata.Subscription {
			_topics[topic] = true
		}
	}

	c.topics = []string{}
	for t, _ := range _topics {
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
			simpleConsumer := &SimpleConsumer{
				topic:       c.topic,
				partitionID: partitionID,
				config:      c.config,
				brokers:     c.brokers,
				belongTO:    c,
				wg:          &c.wg,
			}
			c.simpleConsumers = append(c.simpleConsumers, simpleConsumer)
		}
	}

	return nil
}

// join && set generationID&memberID
func (c *GroupConsumer) join() error {
	glog.Infof("try to join group %s", c.config.GroupID)
	c.memberID = ""
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
		c.config.ClientID, c.config.GroupID, int32(c.config.SessionTimeoutMS), memberID, protocolType, gps)

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
			c.coordinator = nil
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
	glog.V(2).Infof("group assignment:%v", groupAssignment)

	syncGroupResponse, err := c.coordinator.requestSyncGroup(
		c.config.ClientID, c.config.GroupID, c.generationID, c.memberID, groupAssignment)

	if glog.V(2) {
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
	for {
		if c.coordinator == nil {
			err = c.getCoordinator()
		}
		if err != nil {
			glog.Errorf("could not find coordinator: %s", err)
			time.Sleep(time.Millisecond * time.Duration(c.config.RetryBackOffMS))
			continue
		}

		err = c.join()
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
}

func (c *GroupConsumer) heartbeat() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.joined == false {
		return nil
	}

	glog.V(10).Infof("heartbeat generationID:%d memberID:%s", c.generationID, c.memberID)
	_, err := c.coordinator.requestHeartbeat(c.config.ClientID, c.config.GroupID, c.generationID, c.memberID)
	return err
}

func (c *GroupConsumer) CommitOffset(topic string, partitionID int32, offset int64) {
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
		} else {
			glog.Errorf("commit offset %s(%d) [%s][%d]:%d error:%s", c.memberID, c.generationID, topic, partitionID, offset, err)
		}
	} else {
		glog.Errorf("commit offset %s(%d) [%s][%d]:%d error:%s", c.memberID, c.generationID, topic, partitionID, offset, err)
	}
}

func (c *GroupConsumer) stop() {
	if c.simpleConsumers != nil {
		for _, simpleConsumer := range c.simpleConsumers {
			simpleConsumer.Stop()
		}
	}
}

func (c *GroupConsumer) leave() {
	glog.Infof("%s try to leave %s", c.memberID, c.config.GroupID)
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
}

func (c *GroupConsumer) Close() {
	c.stop()
	c.leave()
}

func (gc *GroupConsumer) AwaitClose(timeout time.Duration) {
	c := make(chan bool)
	defer func() {
		select {
		case <-c:
			glog.Info("all simple consumers stopped. return")
			return
		case <-time.After(timeout):
			glog.Info("group consumer await timeout. return")
			return
		}
	}()

	gc.stop()

	go func() {
		gc.wg.Wait()
		c <- true
	}()
}

func (c *GroupConsumer) Consume(fromBeginning bool, messages chan *FullMessage) (chan *FullMessage, error) {
	c.fromBeginning = fromBeginning

	if messages == nil {
		messages = make(chan *FullMessage, 10)
	}
	c.messages = messages

	// go heartbeat
	ticker := time.NewTicker(time.Millisecond * time.Duration(c.config.SessionTimeoutMS) / 10)
	go func() {
		for range ticker.C {
			err := c.heartbeat()
			if err != nil {
				glog.Errorf("failed to send heartbeat:%s", err)
				if err != nil {
					c.stop()
					c.joined = false
					c.consumeWithoutHeartBeat(c.fromBeginning, c.messages)
				}
			}
		}
	}()

	// go refresh topic metadata
	go func() {
		var (
			ticker *time.Ticker = time.NewTicker(time.Millisecond * time.Duration(c.config.MetadataMaxAgeMS))
		)
		for range ticker.C {
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
			if !topicMetadatasSame(c.topicMetadatas, metaDataResponse.TopicMetadatas) {
				c.topicMetadatas = metaDataResponse.TopicMetadatas
				c.stop()
				c.joined = false
				c.consumeWithoutHeartBeat(c.fromBeginning, c.messages)
			}
		}
	}()

	return c.consumeWithoutHeartBeat(c.fromBeginning, c.messages)
}

func (c *GroupConsumer) consumeWithoutHeartBeat(fromBeginning bool, messages chan *FullMessage) (chan *FullMessage, error) {
	var err error
	for {
		err = c.joinAndSync()
		if err == nil {
			break
		} else {
			time.Sleep(time.Millisecond * time.Duration(c.config.RetryBackOffMS))
		}
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
		simpleConsumer.Consume(offset, messages)
	}

	return messages, nil
}

// return true if same
func topicMetadatasSame(a []*TopicMetadata, b []*TopicMetadata) bool {
	if a == nil && a == nil {
		return false
	}

	if a == nil || b == nil {
		return true
	}

	if len(a) != len(b) {
		return true
	}

	for i, _ := range a {
		if a[i].TopicName != b[i].TopicName || a[i].TopicErrorCode != b[i].TopicErrorCode {
			return false
		}

		if len(a[i].PartitionMetadatas) != len(b[i].PartitionMetadatas) {
			return false
		}

		for j, _ := range a[i].PartitionMetadatas {
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
