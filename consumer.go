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
	metadataResponse, err := consumer.brokers.RequestMetaData(consumer.config.ClientID, []string{consumer.topic})
	if err != nil {
		glog.Fatalf("could not get metadata of topic %s:%s", consumer.topic, err)
	}
	glog.V(10).Info(metadataResponse)

	var time int64
	if fromBeginning {
		time = -2
	} else {
		time = -1
	}
	offsetsResponses, err := consumer.brokers.RequestOffsets(consumer.config.ClientID, consumer.topic, -1, time, 1)
	if err != nil {
		glog.Fatalf("could not get offset of topic %s:%s", consumer.topic, err)
	}
	glog.V(10).Info(offsetsResponses)

	consumer.SimpleConsumers = make([]*SimpleConsumer, 0)

	for _, topicMetadatas := range metadataResponse.TopicMetadatas {
		topicName := topicMetadatas.TopicName
		for _, partitionMetadataInfo := range topicMetadatas.PartitionMetadatas {
			partitionID := partitionMetadataInfo.PartitionID
			simpleConsumer := &SimpleConsumer{
				partitionID: partitionID,
				brokers:     consumer.brokers,
				topic:       topicName,
				config:      consumer.config,
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
