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

	stop           bool
	fromBeginning  bool
	offset         int64
	offsetCommited int64

	belongTO *GroupConsumer

	wg *sync.WaitGroup // call ws.Done in defer when Consume return
}

func NewSimpleConsumerWithBrokers(topic string, partitionID int32, config *ConsumerConfig, brokers *Brokers) *SimpleConsumer {
	return &SimpleConsumer{
		config:      config,
		topic:       topic,
		partitionID: partitionID,
		brokers:     brokers,

		wg: &sync.WaitGroup{},
	}
}

func NewSimpleConsumer(topic string, partitionID int32, config *ConsumerConfig) (*SimpleConsumer, error) {
	var err error

	c := &SimpleConsumer{
		config:      config,
		topic:       topic,
		partitionID: partitionID,

		wg: &sync.WaitGroup{},
	}

	brokerConfig := getBrokerConfigFromConsumerConfig(config)

	c.brokers, err = NewBrokers(config.BootstrapServers, config.ClientID, brokerConfig)
	if err != nil {
		return nil, err
	}

	return c, nil
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
			glog.Errorf("could not create broker %d. maybe should refresh metadata.", leaderID)
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
	offsetsResponses, err := c.brokers.RequestOffsets(c.config.ClientID, c.topic, c.partitionID, time, 1)
	if err != nil {
		return -1, err
	}

	return int64(offsetsResponses[0].TopicPartitionOffsets[c.topic][0].Offsets[0]), nil
}

func (c *SimpleConsumer) Stop() {
	c.stop = true
	c.commitOffset()
}

func (c *SimpleConsumer) commitOffset() {
	if c.belongTO != nil {
		if c.belongTO.commitOffset(c.topic, c.partitionID, c.offset) {
			c.offsetCommited = c.offset
		}
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

	glog.V(5).Infof("[%s][%d] offset :%d", c.topic, c.partitionID, c.offset)

	for !c.stop {
		if err = c.getLeaderBroker(); err != nil {
			glog.Errorf("get leader broker error: %s", err)
		} else {
			break
		}
	}

	if c.belongTO != nil && (c.offset == -1 || c.offset == -2) {
		var apiVersion uint16
		if c.config.OffsetsStorage == 0 {
			apiVersion = 0
		} else if c.config.OffsetsStorage == 1 {
			apiVersion = 1
		} else {
			// TODO return error to caller
			glog.Fatalf("invalid config: %s", invallidOffsetsStorageConfig)
			//return messages, invallidOffsetsStorageConfig
		}
		r := NewOffsetFetchRequest(apiVersion, c.config.ClientID, c.belongTO.config.GroupID)
		r.AddPartiton(c.topic, c.partitionID)

		var res *OffsetFetchResponse
		for {
			response, err := c.belongTO.coordinator.Request(r)
			if err != nil {
				glog.Errorf("request fetch offset of [%s][%d] error:%s", c.topic, c.partitionID, err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			res, err = NewOffsetFetchResponse(response)
			if res == nil {
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

	glog.V(5).Infof("[%s][%d] offset :%d", c.topic, c.partitionID, c.offset)

	// offset not fetched from OffsetFetchRequest
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

	if c.belongTO != nil && c.config.AutoCommit {
		ticker := time.NewTicker(time.Millisecond * time.Duration(c.config.AutoCommitIntervalMS))
		go func() {
			for range ticker.C {
				// one messages maybe consumed twice
				if c.stop {
					return
				}
				if c.offset != c.offsetCommited {
					c.commitOffset()
				}
			}
		}()
	}

	go func(messages chan *FullMessage) {
		c.wg.Add(1)

		defer func() {
			glog.V(10).Infof("simple consumer (%s) stop consuming", c.config.ClientID)
			if c.belongTO != nil && c.offset != c.offsetCommited {
				c.commitOffset()
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
						glog.Fatal("buffers still open??")
					}
					glog.V(10).Info("NO more message")
					break
				}
			}

			// is this really needed ?
			if c.belongTO != nil && c.config.CommitAfterFetch && c.offset != c.offsetCommited {
				c.commitOffset()
			}
		}
		c.leaderBroker.Close()
	}(messages)

	return messages, nil
}
