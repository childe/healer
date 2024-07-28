package healer

import (
	"sync"
	"time"
)

// Consumer instance is built to consume messages from kafka broker
type Consumer struct {
	assign map[string][]int
	config ConsumerConfig

	brokers *Brokers
	closed  bool

	simpleConsumers []*SimpleConsumer
	wg              sync.WaitGroup // wg is used to tell if all consumer has already stopped
}

// NewConsumer creates a new consumer instance
func NewConsumer(config interface{}, topics ...string) (*Consumer, error) {
	cfg, err := createConsumerConfig(config)
	logger.Info("create consumer", "origin_config", config, "final_config", cfg)
	if err != nil {
		return nil, err
	}
	brokerConfig := getBrokerConfigFromConsumerConfig(cfg)
	brokers, err := NewBrokersWithConfig(cfg.BootstrapServers, brokerConfig)
	if err != nil {
		return nil, err
	}

	assign := make(map[string][]int)
	for _, topic := range topics {
		assign[topic] = nil
	}

	c := &Consumer{
		config: cfg,
		assign: assign,

		brokers: brokers,
	}

	return c, nil
}

// Subscribe subscribes to the given list of topics, consume all the partitions of the topics.
// Do not call this after calling Consume
func (c *Consumer) Subscribe(topics ...string) {
	c.assign = make(map[string][]int)
	for _, topic := range topics {
		c.assign[topic] = nil
	}
}

// Assign assigns the given partitions to the consumer, the consumer will only consume the given partitions
// Do not call this after calling Consume
func (c *Consumer) Assign(topicPartitons map[string][]int) {
	c.assign = topicPartitons
}

// Consume consumes messages from kafka broker, returns a channel of messages
func (c *Consumer) Consume(messageChan chan *FullMessage) (<-chan *FullMessage, error) {
	var messages chan *FullMessage
	if messageChan == nil {
		messages = make(chan *FullMessage, 100)
	} else {
		messages = messageChan
	}

	var (
		metadataResponse MetadataResponse
		err              error
		topics           []string = make([]string, 0)
	)
	for topicName := range c.assign {
		topics = append(topics, topicName)
	}

	for !c.closed {
		if metadataResponse, err = c.brokers.RequestMetaData(c.config.ClientID, topics); err != nil {
			logger.Error(err, "get metadata failed", "topics", topics)
			time.Sleep(time.Millisecond * 1000)
		} else {
			break
		}
	}
	if c.closed {
		return nil, nil
	}

	c.simpleConsumers = make([]*SimpleConsumer, 0)

	for _, topicMetadatas := range metadataResponse.TopicMetadatas {
		topicName := topicMetadatas.TopicName
		var partitions = make([]int, 0)
		if pids, ok := c.assign[topicName]; !ok || len(pids) == 0 { // consume all partitions
			for _, partitionMetadataInfo := range topicMetadatas.PartitionMetadatas {
				partitions = append(partitions, int(partitionMetadataInfo.PartitionID))
			}
		} else {
			partitions = pids
		}

		for _, p := range partitions {
			simpleConsumer := NewSimpleConsumerWithBrokers(topicName, int32(p), c.config, c.brokers)
			simpleConsumer.wg = &c.wg

			for {
				err := simpleConsumer.getCoordinator()
				if err != nil {
					logger.Error(err, "get coordinator failed")
					time.Sleep(time.Millisecond * time.Duration(c.config.RetryBackOffMS))
					continue
				}
				break
			}
			c.simpleConsumers = append(c.simpleConsumers, simpleConsumer)
		}
	}

	logger.V(4).Info("create simple consumers", "simpleConsumerCount", len(c.simpleConsumers), "consumers", c.simpleConsumers)

	var offset int64
	if c.config.FromBeginning {
		offset = -2
	} else {
		offset = -1
	}

	for _, simpleConsumer := range c.simpleConsumers {
		c.wg.Add(1)
		simpleConsumer.Consume(offset, messages)
	}

	return messages, nil
}

func (c *Consumer) stop() {
	c.closed = true
	if c.simpleConsumers != nil {
		for _, simpleConsumer := range c.simpleConsumers {
			simpleConsumer.Stop()
		}
	}
}

func (consumer *Consumer) AwaitClose(timeout time.Duration) {
	c := make(chan bool)
	defer func() {
		select {
		case <-c:
			logger.Info("all simple consumers stopped. return")
			return
		case <-time.After(timeout):
			logger.Info("consumer await timeout. return")
			return
		}
	}()

	consumer.stop()

	go func() {
		consumer.wg.Wait()
		consumer.brokers.Close()
		c <- true
	}()
}
