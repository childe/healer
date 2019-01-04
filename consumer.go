package healer

import "github.com/golang/glog"

// Consumer instance is built to consume messages from kafka broker
type Consumer struct {
	topic  string
	assign map[string][]int
	config *ConsumerConfig

	brokers *Brokers

	SimpleConsumers []*SimpleConsumer
}

func NewConsumer(topic string, config *ConsumerConfig) (*Consumer, error) {
	var err error
	c := &Consumer{
		config: config,
		topic:  topic,
		assign: nil,
	}
	brokerConfig := getBrokerConfigFromConsumerConfig(config)

	c.brokers, err = NewBrokers(config.BootstrapServers, config.ClientID, brokerConfig)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Consumer) Assign(topicPartitons map[string][]int) {
	c.assign = topicPartitons
}

func (c *Consumer) Consume(fromBeginning bool) (chan *FullMessage, error) {
	// get partitions info
	var (
		metadataResponse *MetadataResponse = nil
		err              error
	)

	if c.assign == nil {
		for {
			if metadataResponse, err = c.brokers.RequestMetaData(c.config.ClientID, []string{c.topic}); err != nil {
				glog.Errorf("could not get metadata of topic %s: %s", c.topic, err)
			} else {
				break
			}
		}
	}

	c.SimpleConsumers = make([]*SimpleConsumer, 0)

	if c.assign == nil {
		for _, topicMetadatas := range metadataResponse.TopicMetadatas {
			topicName := topicMetadatas.TopicName
			for _, partitionMetadataInfo := range topicMetadatas.PartitionMetadatas {
				partitionID := partitionMetadataInfo.PartitionID
				simpleConsumer := NewSimpleConsumerWithBrokers(topicName, partitionID, c.config, c.brokers)
				c.SimpleConsumers = append(c.SimpleConsumers, simpleConsumer)
			}
		}
	} else {
		for topicName, partitions := range c.assign {
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
