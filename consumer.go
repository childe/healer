package healer

import (
	"sync"
	"time"

	"github.com/golang/glog"
)

// Consumer instance is built to consume messages from kafka broker
type Consumer struct {
	assign map[string][]int
	config *ConsumerConfig

	brokers *Brokers

	simpleConsumers []*SimpleConsumer
	wg              sync.WaitGroup // wg is used to tell if all consumer has already stopped
}

func NewConsumer(config *ConsumerConfig, topics ...string) (*Consumer, error) {
	brokerConfig := getBrokerConfigFromConsumerConfig(config)
	brokers, err := NewBrokers(config.BootstrapServers, config.ClientID, brokerConfig)
	if err != nil {
		return nil, err
	}

	assign := make(map[string][]int)
	for _, topic := range topics {
		assign[topic] = nil
	}

	c := &Consumer{
		config: config,
		assign: assign,

		brokers: brokers,
	}

	return c, nil
}

func (c *Consumer) Subscribe(topics ...string) {
	c.assign = make(map[string][]int)
	for _, topic := range topics {
		c.assign[topic] = nil
	}
}

func (c *Consumer) Assign(topicPartitons map[string][]int) {
	c.assign = topicPartitons
}

func (c *Consumer) Consume(messageChan chan *FullMessage) (<-chan *FullMessage, error) {
	var messages chan *FullMessage
	if messageChan == nil {
		messages = make(chan *FullMessage, 10)
	} else {
		messages = messageChan
	}

	var (
		metadataResponse *MetadataResponse = nil
		err              error
		topics           []string = make([]string, 0)
	)
	for topicName, _ := range c.assign {
		topics = append(topics, topicName)
	}

	for {
		if metadataResponse, err = c.brokers.RequestMetaData(c.config.ClientID, topics); err != nil {
			glog.Errorf("could not get metadata of topics %v: %s", topics, err)
			time.Sleep(time.Millisecond * 1000)
		} else {
			break
		}
	}

	c.simpleConsumers = make([]*SimpleConsumer, 0)

	for _, topicMetadatas := range metadataResponse.TopicMetadatas {
		topicName := topicMetadatas.TopicName
		var partitions = make([]int, 0)
		if pids, _ := c.assign[topicName]; pids == nil { // consume all partitions
			for _, partitionMetadataInfo := range topicMetadatas.PartitionMetadatas {
				partitions = append(partitions, int(partitionMetadataInfo.PartitionID))
			}
		} else {
			partitions = pids
		}

		for _, p := range partitions {
			simpleConsumer := &SimpleConsumer{
				topic:       topicName,
				partitionID: int32(p),
				config:      c.config,
				brokers:     c.brokers,
				wg:          &c.wg,
			}

			for {
				err := simpleConsumer.getCoordinator()
				if err != nil {
					glog.Errorf("get coordinator error: %s", err)
					time.Sleep(time.Millisecond * time.Duration(c.config.RetryBackOffMS))
					continue
				}
				break
			}
			c.simpleConsumers = append(c.simpleConsumers, simpleConsumer)
		}
	}

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
			glog.Info("all simple consumers stopped. return")
			return
		case <-time.After(timeout):
			glog.Info("consumer await timeout. return")
			return
		}
	}()

	consumer.stop()

	go func() {
		consumer.wg.Wait()
		c <- true
	}()
}
