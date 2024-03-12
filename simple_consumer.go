package healer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// SimpleConsumer instance is built to consume messages from kafka broker
// TODO make messages have direction
type SimpleConsumer struct {
	topic       string
	partitionID int32
	config      ConsumerConfig

	brokers      *Brokers
	leaderBroker *Broker
	coordinator  *Broker
	partition    PartitionMetadataInfo

	stopChan chan struct{}
	stopWG   sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	stop           bool
	fromBeginning  bool
	offset         int64
	offsetCommited int64

	messages chan *FullMessage

	belongTO *GroupConsumer

	wg *sync.WaitGroup // call wg.Done in defer when Consume return
}

func (c *SimpleConsumer) String() string {
	return fmt.Sprintf("simple-consumer %s-%d", c.topic, c.partitionID)
}

// NewSimpleConsumerWithBrokers create a simple consumer with existing brokers
func NewSimpleConsumerWithBrokers(topic string, partitionID int32, config ConsumerConfig, brokers *Brokers) *SimpleConsumer {
	c := &SimpleConsumer{
		config:      config,
		topic:       topic,
		partitionID: partitionID,
		brokers:     brokers,

		stopChan: make(chan struct{}, 0),
	}
	c.ctx = context.Background()
	c.ctx, c.cancel = context.WithCancel(c.ctx)

	if err := c.refreshPartiton(); err != nil {
		logger.Error(err, "refresh partition meta failed", "topic", c.topic, "partitionID", c.partitionID)
	}

	go func() {
		ticker := time.NewTicker(time.Second * 60 * 1)
		for {
			select {
			case <-ticker.C:
				if err := c.refreshPartiton(); err != nil {
					logger.Error(err, "refresh partition meta failed", "topic", c.topic, "partitionID", c.partitionID)
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
				logger.Error(err, "failed to get coordinator")
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

// NewSimpleConsumer create a simple consumer
func NewSimpleConsumer(topic string, partitionID int32, config interface{}) (*SimpleConsumer, error) {
	cfg, err := createConsumerConfig(config)

	brokerConfig := getBrokerConfigFromConsumerConfig(cfg)

	brokers, err := NewBrokersWithConfig(cfg.BootstrapServers, brokerConfig)
	if err != nil {
		return nil, err
	}

	return NewSimpleConsumerWithBrokers(topic, partitionID, cfg, brokers), nil
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
	logger.Info("get coordinator", "GroupID", c.config.GroupID, "coordinator", coordinatorBroker.address)
	c.coordinator = coordinatorBroker

	return nil
}

var (
	errNoLeader = errors.New("not leader found")
)

// set c.leaderBroker
func (c *SimpleConsumer) getLeaderBroker() error {
	var (
		err          error
		leaderID     int32
		leaderBroker *Broker
	)

	leaderID, err = c.brokers.findLeader(c.config.ClientID, c.topic, c.partitionID)
	if err != nil {
		return fmt.Errorf("find leader for %s-%d error: %w", c.topic, c.partitionID, err)
	}

	logger.Info("get leader", "topic", c.topic, "partitionID", c.partitionID, "leaderID", leaderID)
	if leaderID == -1 {
		return errNoLeader
	}

	leaderBroker, err = c.brokers.NewBroker(leaderID)
	if err != nil {
		// FIXME refresh metadata
		logger.Error(err, "could not create broker. maybe should refresh metadata.", "leaderID", leaderID, "leaderAddress", c.brokers.brokers[leaderID].address)
		return err
	}

	c.leaderBroker = leaderBroker
	logger.Info("leader broker created", "leaderID", leaderID, "leaderAddress", c.leaderBroker.GetAddress(), "topic", c.topic, "partitionID", c.partitionID)
	return nil
}

// init offset based on fromBeginning if not got commited offset
func (c *SimpleConsumer) initOffset() {
	logger.V(1).Info("init offset", "topic", c.topic, "partitionID", c.partitionID, "offset", c.offset)

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
			logger.Error(err, "could not get offset", "topic", c.topic, "partitionID", c.partitionID)
			time.Sleep(time.Millisecond * time.Duration(c.config.RetryBackOffMS))
		} else {
			logger.Info("fetched offset", "topic", c.topic, "partitionID", c.partitionID, "offset", c.offset)
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
	if err := offsetsResponse.Error(); err != nil {
		return -1, err
	}
	return int64(offsetsResponse.TopicPartitionOffsets[c.topic][0].Offsets[0]), nil
}

func (c *SimpleConsumer) getCommitedOffet() error {
	logger.Info("get commited offset", "topic", c.topic, "partitionID", c.partitionID)
	var apiVersion uint16
	if c.config.OffsetsStorage == 0 {
		apiVersion = 0
	} else if c.config.OffsetsStorage == 1 {
		apiVersion = 1
	} else {
		return errors.New("invalid offsetStorage config")
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
			logger.Error(err, "failed to request fetch offset, sleep 500 ms", "topic", c.topic, "partitionID", c.partitionID)
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
	return nil
}

// Stop the consumer and wait for all relating go-routines to exit
func (c *SimpleConsumer) Stop() {
	logger.Info("stopping simple consumer", "topic", c.topic, "partitionID", c.partitionID)
	c.stop = true
	c.cancel()

	close(c.stopChan)
	c.stopWG.Wait()

}

// call this when simpleConsumer NOT belong to GroupConsumer, or call BelongTo.Commit()
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
		logger.V(3).Info("offset committed", "GroupID", c.config.GroupID, "topic", c.topic, "partitionID", c.partitionID, "offset", c.offset)
		return true
	}
	logger.Error(err, "commit offset failed", "GroupID", c.config.GroupID, "topic", c.topic, "partitionID", c.partitionID, "offset", c.offset)
	return false
}

// CommitOffset commit offset to coordinator
// if simpleConsumer belong to a GroupConsumer, it uses groupconsumer to commit
// else if it has GroupId, it use its own coordinator to commit
func (c *SimpleConsumer) CommitOffset() {
	if c.offset == c.offsetCommited {
		logger.V(3).Info("current offset does not change, skip committing", "offset", c.offset, "topic", c.topic, "partitionID", c.partitionID)
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

// Consume begins to fetch messages.
// It create and return a new channel if you pass nil, or it returns the channel you passed.
func (c *SimpleConsumer) Consume(offset int64, messageChan chan *FullMessage) (<-chan *FullMessage, error) {
	var (
		err      error
		messages = messageChan
	)
	if messageChan == nil {
		messages = make(chan *FullMessage, 100)
	}
	c.messages = messages

	c.offset = offset

	logger.V(5).Info("start consume from offset (before fetch offset)", "topic", c.topic, "partitionID", c.partitionID, "offset", c.offset)

	for !c.stop {
		if err = c.getLeaderBroker(); err != nil {
			logger.Error(err, "get leader broker of [%s/%d] error: %s", "topic", c.topic, "partitionID", c.partitionID)
		} else {
			break
		}
	}

	if c.stop {
		return messages, nil
	}

	if c.config.GroupID != "" && (c.offset == -1 || c.offset == -2) {
		c.getCommitedOffet()
	}
	logger.V(3).Info("got committed offset", "topic", c.topic, "partitionID", c.partitionID, "offset", c.offset)

	if c.offset == -1 || c.offset == -2 {
		c.initOffset()
		logger.V(3).Info("[%s][%d] offset: %d (after init offset)", "topic", c.topic, "partitionID", c.partitionID, "offset", c.offset)
	}

	if c.config.AutoCommit && c.config.GroupID != "" {
		go func() {
			ticker := time.NewTicker(time.Millisecond * time.Duration(c.config.AutoCommitIntervalMS))
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					c.CommitOffset()
				case <-c.ctx.Done():
					c.CommitOffset()
					return
				}
			}
		}()
	}

	go c.consumeLoop(messages)

	return messages, nil
}

func (c *SimpleConsumer) consumeLoop(messages chan *FullMessage) {
	defer func() {
		logger.V(5).Info("simple consumer stop consuming", "topic", c.topic, "partitionID", c.partitionID)
		if c.leaderBroker != nil {
			c.leaderBroker.Close()
		}
		if c.wg != nil {
			c.wg.Done()
		}
	}()

	buffers := make(chan []byte, 10)
	innerMessages := make(chan *FullMessage, 1)

	for !c.stop {
		// fetch
		go func() {
			c.stopWG.Add(1)
			defer func() {
				c.stopWG.Done()
			}()

			logger.V(5).Info("send fetch request", "topic", c.topic, "partitionID", c.partitionID, "offset", c.offset)
			r := NewFetchRequest(c.config.ClientID, c.config.FetchMaxWaitMS, c.config.FetchMinBytes)
			r.addPartition(c.topic, c.partitionID, c.offset, c.config.FetchMaxBytes, c.partition.LeaderEpoch)

			err := c.leaderBroker.requestFetchStreamingly(c.ctx, r, buffers)
			if err != nil {
				if err == context.Canceled {
					return
				}
				logger.Error(err, "failed to send fetch request")
			}
		}()

		//decode
		go func() {
			c.stopWG.Add(1)
			defer func() {
				c.stopWG.Done()
			}()

			frsd := fetchResponseStreamDecoder{
				ctx:      c.ctx,
				buffers:  buffers,
				messages: innerMessages,
				version:  c.leaderBroker.getHighestAvailableAPIVersion(API_FetchRequest),
			}

			if err := frsd.streamDecode(c.leaderBroker.getHighestAvailableAPIVersion(API_FetchRequest), c.offset); err != nil {
				if err == context.Canceled {
					return
				}
				logger.Error(err, "failed to decode fetch response")
			}
		}()

		// consume all messages from one fetch response
		c.consumeFromOneFetchRequest(innerMessages, messages)

	}
}

func (c *SimpleConsumer) consumeFromOneFetchRequest(innerMessages chan *FullMessage, messages chan *FullMessage) (err error) {
	c.stopWG.Add(1)
	defer func() {
		c.stopWG.Done()
	}()

	var message *FullMessage
	for !c.stop {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case message = <-innerMessages:
		}
		if message != nil {
			if message.Error != nil {
				logger.Error(message.Error, "message error", "topic", c.topic, "partitionID", c.partitionID)
				if message.Error == &maxBytesTooSmall {
					// TODO user custom config, if maxBytesTooSmall, double it
					c.config.FetchMaxBytes *= 2
					logger.Info("fetch.max.bytes is too small, double it", "new FetchMaxBytes", c.config.FetchMaxBytes)
				}
				if message.Error == AllError[1] {
					c.offset, err = c.getOffset(c.fromBeginning)
					if err != nil {
						logger.Error(err, "failed to get offset", "topic", c.topic, "partitionID", c.partitionID)
					}
				} else if message.Error == AllError[6] {
					c.leaderBroker.Close()
					c.leaderBroker = nil
					for !c.stop {
						if err = c.getLeaderBroker(); err != nil {
							logger.Error(err, "failer to get leader", "topic", c.topic, "partitionID", c.partitionID)
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
			logger.V(5).Info("consumed all messages from one fetch response", "currentOffset", c.offset)
			break
		}
	}
	return nil
}
