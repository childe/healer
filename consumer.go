package healer

import (
	"time"

	"github.com/golang/glog"
)

// Consumer instance is built to consume messages from kafka broker
type Consumer struct {
	assign map[string][]int
	config *ConsumerConfig

	brokers *Brokers

	SimpleConsumers []*SimpleConsumer
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

func (c *Consumer) Consume(fromBeginning bool) (chan *FullMessage, error) {
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

	c.SimpleConsumers = make([]*SimpleConsumer, 0)

	for _, topicMetadatas := range metadataResponse.TopicMetadatas {
		topicName := topicMetadatas.TopicName
		if partitions, _ := c.assign[topicName]; partitions == nil { // consume all partitions
			for _, partitionMetadataInfo := range topicMetadatas.PartitionMetadatas {
				partitionID := partitionMetadataInfo.PartitionID
				simpleConsumer := NewSimpleConsumerWithBrokers(topicName, partitionID, c.config, c.brokers)
				c.SimpleConsumers = append(c.SimpleConsumers, simpleConsumer)
			}
		} else {
			for _, p := range partitions {
				simpleConsumer := NewSimpleConsumerWithBrokers(topicName, int32(p), c.config, c.brokers)
				c.SimpleConsumers = append(c.SimpleConsumers, simpleConsumer)
			}
		}
	}

	var offset int64
	if fromBeginning {
		offset = -2
	} else {
		offset = -1
	}

	messages := make(chan *FullMessage, 10)
	for _, simpleConsumer := range c.SimpleConsumers {
		simpleConsumer.Consume(offset, messages)
	}

	return messages, nil
}
