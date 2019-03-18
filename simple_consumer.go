package healer

import (
	"errors"
	"sync"
	"time"

	"github.com/golang/glog"
)

// SimpleConsumer instance is built to consume messages from kafka broker
type SimpleConsumer struct {
	topic       string
	partitionID int32
	config      *ConsumerConfig

	brokers      *Brokers
	leaderBroker *Broker
	coordinator  *Broker

	stop           bool
	fromBeginning  bool
	offset         int64
	offsetCommited int64

	belongTO *GroupConsumer

	wg *sync.WaitGroup // call wg.Done in defer when Consume return
}

func NewSimpleConsumerWithBrokers(topic string, partitionID int32, config *ConsumerConfig, brokers *Brokers) *SimpleConsumer {
	c := &SimpleConsumer{
		config:      config,
		topic:       topic,
		partitionID: partitionID,
		brokers:     brokers,

		wg: &sync.WaitGroup{},
	}

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

	brokers, err := NewBrokers(config.BootstrapServers, config.ClientID, brokerConfig)
	if err != nil {
		return nil, err
	}

	return NewSimpleConsumerWithBrokers(topic, partitionID, config, brokers), nil
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
		err      error
		leaderID int32
	)

	leaderID, err = c.brokers.findLeader(c.config.ClientID, c.topic, c.partitionID)
	if err != nil {
		return err
	}

	glog.V(5).Infof("leader ID of [%s][%d] is %d", c.topic, c.partitionID, leaderID)
	if leaderID == -1 {
		return invalidLeaderIDError
	}

	var retry = 3
	for i := 0; i < retry; i++ {
		c.leaderBroker, err = c.brokers.NewBroker(leaderID)
		if err != nil {
			// TODO refresh metadata?
			glog.Errorf("could not create broker %d[%s]. maybe should refresh metadata.", leaderID, c.brokers.brokers[leaderID].address)
		} else {
			glog.V(5).Infof("got leader broker %s with id %d", c.leaderBroker.address, leaderID)
			return nil
		}
	}
	return err
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
		res         *OffsetFetchResponse
		coordinator *Broker
	)

	if c.belongTO != nil {
		coordinator = c.belongTO.coordinator
	} else {
		coordinator = c.coordinator
	}

	for {
		response, err := coordinator.Request(r)
		if err != nil {
			glog.Errorf("request fetch offset of [%s][%d] error:%s", c.topic, c.partitionID, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		res, err = NewOffsetFetchResponse(response)
		if err != nil {
			glog.Errorf("decode offset fetch response error:%s", err)
			time.Sleep(500 * time.Millisecond)
		} else {
			break
		}
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
	c.CommitOffset()
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

	payload, err := c.coordinator.Request(offsetComimtReq)
	if err == nil {
		_, err := NewOffsetCommitResponse(payload)
		if err == nil {
			glog.V(5).Infof("commit offset %s [%s][%d]:%d", c.config.GroupID, c.topic, c.partitionID, c.offset)
			return true
		} else {
			glog.Errorf("commit offset %s [%s][%d]:%d error: %s", c.config.GroupID, c.topic, c.partitionID, c.offset, err)
		}
	} else {
		glog.Errorf("commit offset %s [%s][%d]:%d error: %s", c.config.GroupID, c.topic, c.partitionID, c.offset, err)
	}
	return false
}

func (c *SimpleConsumer) CommitOffset() {
	if c.belongTO != nil {
		if c.belongTO.commitOffset(c.topic, c.partitionID, c.offset) {
			c.offsetCommited = c.offset
		}
	} else if c.config.GroupID != "" {
		c.commitOffset()
	}
}

func (c *SimpleConsumer) Consume(offset int64, messageChan chan *FullMessage) (chan *FullMessage, error) {
	var messages chan *FullMessage
	if messageChan == nil {
		messages = make(chan *FullMessage, 10)
	} else {
		messages = messageChan
	}

	var err error

	c.stop = false
	c.offset = offset

	glog.V(5).Infof("[%s][%d] offset :%d (before fetch offset)", c.topic, c.partitionID, c.offset)

	for !c.stop {
		if err = c.getLeaderBroker(); err != nil {
			glog.Errorf("get leader broker error: %s", err)
		} else {
			break
		}
	}

	if c.stop {
		c.wg.Done()
		return messages, nil
	}

	if c.config.GroupID != "" && (c.offset == -1 || c.offset == -2) {
		c.getCommitedOffet()
	}

	glog.V(5).Infof("[%s][%d] offset :%d", c.topic, c.partitionID, c.offset)

	// offset not fetched from OffsetFetchRequest
	if c.offset < 0 {
		if c.offset == -1 {
			c.fromBeginning = false
		} else if c.offset == -2 {
			c.fromBeginning = true
		}

		for !c.stop {
			if c.offset, err = c.getOffset(c.fromBeginning); err != nil {
				glog.Errorf("could not get offset %s[%d]:%s", c.topic, c.partitionID, err)
			} else {
				glog.Infof("consume [%s][%d] from %d", c.topic, c.partitionID, c.offset)
				break
			}
		}
	}

	if c.config.AutoCommit && c.config.GroupID != "" {
		ticker := time.NewTicker(time.Millisecond * time.Duration(c.config.AutoCommitIntervalMS))
		go func() {
			for range ticker.C {
				// one messages maybe consumed twice
				if c.stop {
					return
				}
				if c.offset != c.offsetCommited {
					c.CommitOffset()
				}
			}
		}()
	}

	go func(messages chan *FullMessage) {
		defer func() {
			glog.V(5).Infof("simple consumer stop consuming %s[%d]", c.topic, c.partitionID)
			if c.belongTO != nil && c.offset != c.offsetCommited {
				c.CommitOffset()
			}
			c.wg.Done()
		}()

		for c.stop == false {
			// TODO set CorrelationID to 0 firstly and then set by broker
			fetchRequest := NewFetchRequest(c.config.ClientID, c.config.FetchMaxWaitMS, c.config.FetchMinBytes)
			fetchRequest.addPartition(c.topic, c.partitionID, c.offset, c.config.FetchMaxBytes)

			buffers := make(chan []byte, 10)
			innerMessages := make(chan *FullMessage, 10)
			go func() {
				err := c.leaderBroker.requestFetchStreamingly(fetchRequest, buffers)
				if err != nil {
					glog.Errorf("fetch error:%s", err)
				}
			}()

			fetchResponseStreamDecoder := FetchResponseStreamDecoder{
				totalLength: 0,
				length:      0,
				buffers:     buffers,
				messages:    innerMessages,
				more:        true,
			}
			go fetchResponseStreamDecoder.consumeFetchResponse()
			for c.stop == false {
				message, more := <-innerMessages
				if more {
					if message.Error != nil {
						glog.Infof("consumer %s[%d] error:%s", c.topic, c.partitionID, message.Error)
						if message.Error == AllError[1] {
							c.offset, err = c.getOffset(c.fromBeginning)
							if err != nil {
								glog.Errorf("could not get %s[%d] offset:%s", c.topic, c.partitionID, message.Error)
							}
						} else if message.Error == AllError[6] {
							for !c.stop {
								if err = c.getLeaderBroker(); err != nil {
									glog.Errorf("get leader broker of [%s/%d] error: %s", c.topic, c.partitionID, err)
								} else {
									break
								}
							}
						}
					} else {
						c.offset = message.Message.Offset + 1
						messages <- message
					}
				} else {
					if buffer, ok := <-buffers; ok {
						//glog.Info(buffer)
						glog.Info(len(buffer))
						panic("buffers still open??")
					}
					glog.V(10).Info("NO more message")
					break
				}
			}

			// is this really needed ?
			if c.belongTO != nil && c.config.CommitAfterFetch && c.offset != c.offsetCommited {
				c.CommitOffset()
			}
		}
		c.leaderBroker.Close()
	}(messages)

	return messages, nil
}
