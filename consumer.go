package healer

import "github.com/golang/glog"

// Consumer instance is built to consume messages from kafka broker
type Consumer struct {
	ClientID    string
	Brokers     *Brokers
	BrokerList  string
	TopicName   string
	MaxBytes    int32
	MaxWaitTime int32
	MinBytes    int32

	SimpleConsumers []*SimpleConsumer
}

func NewConsumer(brokers *Brokers) *Consumer {
	return nil
}

func (consumer *Consumer) Consume(fromBeginning bool) (chan *FullMessage, error) {
	// get partitions info
	metadataResponse, err := consumer.Brokers.RequestMetaData(consumer.ClientID, []string{consumer.TopicName})
	if err != nil {
		glog.Fatalf("could not get metadata of topic %s:%s", consumer.TopicName, err)
	}
	glog.V(10).Info(metadataResponse)

	var time int64
	if fromBeginning {
		time = -2
	} else {
		time = -1
	}
	offsetsResponses, err := consumer.Brokers.RequestOffsets(consumer.ClientID, consumer.TopicName, -1, time, 1)
	if err != nil {
		glog.Fatalf("could not get offset of topic %s:%s", consumer.TopicName, err)
	}
	glog.V(10).Info(offsetsResponses)

	consumer.SimpleConsumers = make([]*SimpleConsumer, 0)

	for _, topicMetadatas := range metadataResponse.TopicMetadatas {
		topicName := topicMetadatas.TopicName
		for _, partitionMetadataInfo := range topicMetadatas.PartitionMetadatas {
			partitionID := int32(partitionMetadataInfo.PartitionId)
			simpleConsumer := &SimpleConsumer{
				Partition:   partitionID,
				Brokers:     consumer.Brokers,
				TopicName:   topicName,
				ClientID:    consumer.ClientID,
				MaxBytes:    consumer.MaxBytes,
				MaxWaitTime: consumer.MaxWaitTime,
				MinBytes:    consumer.MinBytes,
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
