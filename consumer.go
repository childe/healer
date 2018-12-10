package healer

import "github.com/golang/glog"

// Consumer instance is built to consume messages from kafka broker
type Consumer struct {
	topic  string
	config *ConsumerConfig

	brokers *Brokers

	SimpleConsumers []*SimpleConsumer
}

func NewConsumer(topic string, config *ConsumerConfig) (*Consumer, error) {
	if err := validateConsumerConfig(config); err != nil {
		return nil, err
	}
	var err error
	c := &Consumer{
		config: config,
		topic:  topic,
	}
	brokerConfig := getBrokerConfigFromConsumerConfig(config)

	c.brokers, err = NewBrokers(config.BootstrapServers, config.ClientID, brokerConfig)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (consumer *Consumer) Consume(fromBeginning bool) (chan *FullMessage, error) {
	// get partitions info
	var (
		metadataResponse *MetadataResponse
		err              error
	)
	for {
		if metadataResponse, err = consumer.brokers.RequestMetaData(consumer.config.ClientID, []string{consumer.topic}); err != nil {
			glog.Errorf("could not get metadata of topic %s: %s", consumer.topic, err)
		} else {
			break
		}
	}

	consumer.SimpleConsumers = make([]*SimpleConsumer, 0)

	for _, topicMetadatas := range metadataResponse.TopicMetadatas {
		topicName := topicMetadatas.TopicName
		for _, partitionMetadataInfo := range topicMetadatas.PartitionMetadatas {
			partitionID := partitionMetadataInfo.PartitionID
			simpleConsumer, err := NewSimpleConsumerWithBrokers(topicName, partitionID, consumer.config, consumer.brokers)
			if err != nil {
				return nil, err
			}
			consumer.SimpleConsumers = append(consumer.SimpleConsumers, simpleConsumer)
		}
	}

	var offset int64
	if fromBeginning {
		offset = -2
	} else {
		offset = -1
	}

	messages := make(chan *FullMessage, 10)
	for _, simpleConsumer := range consumer.SimpleConsumers {
		simpleConsumer.Consume(offset, messages)
	}

	return messages, nil
}

func validateConsumerConfig(config *ConsumerConfig) error {
	if config.OffsetsStorage != 0 && config.OffsetsStorage != 1 {
		return invallidOffsetsStorageConfig
	}
	return nil
}
