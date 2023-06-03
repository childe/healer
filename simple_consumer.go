package healer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/golang/glog"
)

// SimpleConsumer instance is built to consume messages from kafka broker
// TODO make messages have direction
type SimpleConsumer struct {
	topic       string
	partitionID int32
	config      *ConsumerConfig

	brokers      *Brokers
	leaderBroker *Broker
	coordinator  *Broker
	partition    PartitionMetadataInfo
	stopChan     chan struct{}

	stop           bool
	fromBeginning  bool
	offset         int64
	offsetCommited int64

	stopChanForCommit chan bool

	messages chan *FullMessage

	belongTO *GroupConsumer

	wg *sync.WaitGroup // call wg.Done in defer when Consume return
}

func NewSimpleConsumerWithBrokers(topic string, partitionID int32, config *ConsumerConfig, brokers *Brokers) *SimpleConsumer {
	c := &SimpleConsumer{
		config:      config,
		topic:       topic,
		partitionID: partitionID,
		brokers:     brokers,

		stopChan:          make(chan struct{}, 0),
		stopChanForCommit: make(chan bool, 1),
	}

	if err := c.refreshPartiton(); err != nil {
		glog.Errorf("refresh metadata in simple consumer for %s[%d] error: %s", c.topic, c.partitionID, err)
	}
	go func() {
		ticker := time.NewTicker(time.Second * 60 * 1)
		for {
			select {
			case <-ticker.C:
				if err := c.refreshPartiton(); err != nil {
					glog.Errorf("refresh metadata in simple consumer for %s[%d] error: %s", c.topic, c.partitionID, err)
				}
			case <-c.stopChan:
				return
			}
		}
	}()

	if config.GroupID != "" {
		var err error
		for {
			err = c.getCoordinator()
			if err != nil {
				glog.Errorf("get coordinator error: %s", err)
				time.Sleep(time.Millisecond * time.Duration(c.config.RetryBackOffMS))
				continue
			}
			break
		}
	} else {
		c.coordinator = nil
	}

	return c
}

func NewSimpleConsumer(topic string, partitionID int32, config *ConsumerConfig) (*SimpleConsumer, error) {
	var err error

	brokerConfig := getBrokerConfigFromConsumerConfig(config)

	brokers, err := NewBrokersWithConfig(config.BootstrapServers, brokerConfig)
	if err != nil {
		return nil, err
	}

	return NewSimpleConsumerWithBrokers(topic, partitionID, config, brokers), nil
}

func (c *SimpleConsumer) refreshPartiton() error {
	metaDataResponse, err := c.brokers.RequestMetaData(c.config.ClientID, []string{c.topic})
	if err != nil {
		return err
	}
	for _, topic := range metaDataResponse.TopicMetadatas {
		for _, p := range topic.PartitionMetadatas {
			if p.PartitionID == c.partitionID {
				c.partition = *p
				return nil
			}
		}
	}
	return errors.New("partition not found in meetadata response")
}
func (c *SimpleConsumer) getCoordinator() error {
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

var (
	invalidLeaderIDError = errors.New("leaderID of topic/partition must not be -1")
)

// TOOD put retry in Request
func (c *SimpleConsumer) getLeaderBroker() error {
	var (
		err          error
		leaderID     int32
		leaderBroker *Broker
	)

	leaderID, err = c.brokers.findLeader(c.config.ClientID, c.topic, c.partitionID)
	if err != nil {
		return err
	}

	glog.V(5).Infof("leader ID of [%s][%d] is %d", c.topic, c.partitionID, leaderID)
	if leaderID == -1 {
		return invalidLeaderIDError
	}

	leaderBroker, err = c.brokers.NewBroker(leaderID)
	if err != nil {
		// TODO refresh metadata?
		glog.Errorf("could not create broker %d[%s]. maybe should refresh metadata.", leaderID, c.brokers.brokers[leaderID].address)
		return err
	}

	c.leaderBroker = leaderBroker
	glog.V(5).Infof("got leader broker %s with id %d", c.leaderBroker.address, leaderID)
	return nil
}

// offset not fetched from OffsetFetchRequest
func (c *SimpleConsumer) initOffset() {
	glog.V(5).Infof("[%s][%d] offset: %d", c.topic, c.partitionID, c.offset)

	if c.offset >= 0 {
		return
	}

	var err error
	if c.offset == -1 {
		c.fromBeginning = false
	} else if c.offset == -2 {
		c.fromBeginning = true
	}

	for !c.stop {
		if c.offset, err = c.getOffset(c.fromBeginning); err != nil {
			glog.Errorf("could not get offset %s[%d]:%s", c.topic, c.partitionID, err)
			time.Sleep(time.Millisecond * time.Duration(c.config.RetryBackOffMS))
		} else {
			glog.Infof("consume [%s][%d] from %d", c.topic, c.partitionID, c.offset)
			break
		}
	}
}

func (c *SimpleConsumer) getOffset(fromBeginning bool) (int64, error) {
	var time int64
	if fromBeginning {
		time = -2
	} else {
		time = -1
	}

	offsetsResponse, err := c.leaderBroker.requestOffsets(c.config.ClientID, c.topic, []int32{c.partitionID}, time, 1)
	if err != nil {
		return -1, err
	}
	return int64(offsetsResponse.TopicPartitionOffsets[c.topic][0].Offsets[0]), nil
}

func (c *SimpleConsumer) getCommitedOffet() {
	var apiVersion uint16
	if c.config.OffsetsStorage == 0 {
		apiVersion = 0
	} else if c.config.OffsetsStorage == 1 {
		apiVersion = 1
	} else {
		// TODO return error to caller
		panic("invalid offsetStorage config")
		//return messages, invallidOffsetsStorageConfig
	}
	r := NewOffsetFetchRequest(apiVersion, c.config.ClientID, c.config.GroupID)
	r.AddPartiton(c.topic, c.partitionID)

	var (
		res         OffsetFetchResponse
		coordinator *Broker
	)

	if c.belongTO != nil {
		coordinator = c.belongTO.coordinator
	} else {
		coordinator = c.coordinator
	}

	for {
		resp, err := coordinator.RequestAndGet(r)
		if err != nil {
			glog.Errorf("request fetch offset of [%s][%d] error:%s, sleep 500 ms", c.topic, c.partitionID, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		res = resp.(OffsetFetchResponse)
		break
	}

	for _, t := range res.Topics {
		if t.Topic != c.topic {
			continue
		}
		for _, p := range t.Partitions {
			if int32(p.PartitionID) == c.partitionID {
				if p.Offset >= 0 {
					c.offset = p.Offset
				}
				break
			}
		}
	}
}

func (c *SimpleConsumer) Stop() {
	c.stop = true
	if c.config.AutoCommit {
		c.CommitOffset()
	}
	c.stopChan <- struct{}{}
}

// when NOT belong to GroupConsumer
func (c *SimpleConsumer) commitOffset() bool {
	var apiVersion uint16
	if c.config.OffsetsStorage == 1 {
		apiVersion = 2
	} else {
		apiVersion = 0
	}

	offsetComimtReq := NewOffsetCommitRequest(apiVersion, c.config.ClientID, c.config.GroupID)
	offsetComimtReq.SetMemberID("")
	offsetComimtReq.SetGenerationID(-1)
	offsetComimtReq.SetRetentionTime(-1)
	offsetComimtReq.AddPartiton(c.topic, c.partitionID, c.offset, "")

	_, err := c.coordinator.RequestAndGet(offsetComimtReq)
	if err == nil {
		glog.V(5).Infof("commit offset %s [%s][%d]:%d", c.config.GroupID, c.topic, c.partitionID, c.offset)
		return true
	}
	glog.Errorf("commit offset %s [%s][%d]:%d error: %s", c.config.GroupID, c.topic, c.partitionID, c.offset, err)
	return false
}

// CommitOffset commit offset to coordinator
// if simpleConsumer belong to a GroupConsumer, it uses groupconsumer to commit
// else if it has GroupId, it use its own coordinator to commit
func (c *SimpleConsumer) CommitOffset() {
	if c.offset == c.offsetCommited {
		if glog.V(5) {
			glog.Infof("current offset[%d] of %s[%d] does not change, skip committing", c.offset, c.topic, c.partitionID)
		}
		return
	}
	offset := c.offset
	if c.belongTO != nil {
		if c.belongTO.commitOffset(c.topic, c.partitionID, offset) {
			c.offsetCommited = offset
		}
	} else if c.config.GroupID != "" {
		if c.commitOffset() {
			c.offsetCommited = offset
		}
	}
}

func (c *SimpleConsumer) Consume(offset int64, messageChan chan *FullMessage) (<-chan *FullMessage, error) {
	var messages chan *FullMessage
	if messageChan == nil {
		messages = make(chan *FullMessage, 100)
	} else {
		messages = messageChan
	}

	c.messages = messages

	var err error

	c.offset = offset

	glog.V(5).Infof("[%s][%d] offset: %d (before fetch offset)", c.topic, c.partitionID, c.offset)

	for !c.stop {
		if err = c.getLeaderBroker(); err != nil {
			glog.Errorf("get leader broker error: %s", err)
		} else {
			break
		}
	}

	if c.stop {
		if c.leaderBroker != nil {
			c.leaderBroker.Close()
		}
		if c.wg != nil {
			c.wg.Done()
		}
		return messages, nil
	}

	if c.config.GroupID != "" && (c.offset == -1 || c.offset == -2) {
		c.getCommitedOffet()
	}

	c.initOffset()

	if c.config.AutoCommit && c.config.GroupID != "" {
		ticker := time.NewTicker(time.Millisecond * time.Duration(c.config.AutoCommitIntervalMS))
		go func() {
			for {
				select {
				case <-ticker.C:
					c.CommitOffset()
				case <-c.stopChanForCommit:
					c.CommitOffset()
					ticker.Stop()
					return
				}
			}
		}()
	}

	go func(messages chan *FullMessage) {
		defer func() {
			c.stopChanForCommit <- true
			glog.V(5).Infof("simple consumer stop consuming %s[%d]", c.topic, c.partitionID)
			if c.wg != nil {
				c.wg.Done()
			}
		}()

		defer func() {
			if c.leaderBroker != nil {
				c.leaderBroker.Close()
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		// fetch will stop sending kafka response to api after cancel
		defer cancel()

		buffers := make(chan []byte, 10)
		innerMessages := make(chan *FullMessage, 1)

		fetchWG := sync.WaitGroup{}
		decodeWG := sync.WaitGroup{}
		// 如果使用 stop 来决定两个 Goroutine 的停止, 可能会decode先停了, fetch 永远卡死了
		consumeDoneChan := false

		fetchStarted := make(chan bool, 1)

		// call fetch api
		go func() {
			fetchStarted <- true

			for {
				fetchWG.Add(1)
				r := NewFetchRequest(c.config.ClientID, c.config.FetchMaxWaitMS, c.config.FetchMinBytes)
				r.addPartition(c.topic, c.partitionID, c.offset, c.config.FetchMaxBytes, c.partition.LeaderEpoch)

				err := c.leaderBroker.requestFetchStreamingly(ctx, r, buffers)
				if err != nil {
					glog.Errorf("fetch error:%s", err)
					time.Sleep(time.Millisecond * time.Duration(c.config.RetryBackOffMS))
				}
				fetchWG.Wait()
				if consumeDoneChan {
					return
				}
			}
		}()

		// decode
		go func() {
			frsd := fetchResponseStreamDecoder{
				buffers:  buffers,
				messages: innerMessages,
				version:  c.leaderBroker.getHighestAvailableAPIVersion(API_FetchRequest),
			}

			for {
				decodeWG.Add(1)
				frsd.streamDecode(c.leaderBroker.getHighestAvailableAPIVersion(API_FetchRequest), c.offset)
				decodeWG.Wait()
				if consumeDoneChan {
					return
				}
			}
		}()

		<-fetchStarted
		for {
			for c.stop == false { // consume all messages from one fetch response
				message := <-innerMessages
				if message != nil {
					if message.Error != nil {
						glog.Errorf("consumer %s[%d] error:%s", c.topic, c.partitionID, message.Error)
						if message.Error == &maxBytesTooSmall {
							c.config.FetchMaxBytes *= 2
							glog.Infof("fetch.max.bytes is too small, double it to %d", c.config.FetchMaxBytes)
						}
						if message.Error == AllError[1] {
							c.offset, err = c.getOffset(c.fromBeginning)
							if err != nil {
								glog.Errorf("could not get %s[%d] offset:%s", c.topic, c.partitionID, message.Error)
							}
						} else if message.Error == AllError[6] {
							c.leaderBroker.Close()
							c.leaderBroker = nil
							for !c.stop {
								if err = c.getLeaderBroker(); err != nil {
									glog.Errorf("get leader broker of [%s/%d] error: %s", c.topic, c.partitionID, err)
								} else {
									break
								}
							}
						}
					} else {
						messages <- message
						c.offset = message.Message.Offset + 1
					}
				} else {
					if glog.V(15) {
						glog.Infof("consumed all messages from one fetch response. current offset: %d", c.offset)
					}
					break
				}
			}

			if c.stop {
				consumeDoneChan = true
				fetchWG.Done()
				decodeWG.Done()
				break
			} else {
				fetchWG.Done()
				decodeWG.Done()
			}
		}
	}(messages)

	return messages, nil
}
