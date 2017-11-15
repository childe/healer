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

	SimpleConsumer2s map[int32]*SimpleConsumer2 // leaderID:*SimpleConsumer2
}

func NewConsumer(brokers *Brokers) *Consumer {
	return nil
}

func findOffset(topicName string, partitionID int32, offsetsResponses []*OffsetsResponse) int64 {
	for _, offsetsResponse := range offsetsResponses {
		for _topicName, partitionOffsets := range offsetsResponse.Info {
			if topicName != _topicName {
				continue
			}
			for _, partitionOffset := range partitionOffsets {
				if int32(partitionOffset.Partition) == partitionID {
					if len(partitionOffset.Offset) > 0 {
						return int64(partitionOffset.Offset[0])
					}
				}
			}
		}
	}
	return -1
}

func (consumer *Consumer) Consume(fromBeginning bool) (chan *FullMessage, error) {
	// get partitions info
	metadataResponse, err := consumer.Brokers.RequestMetaData(consumer.ClientID, &consumer.TopicName)
	if err != nil {
		glog.Fatal("could not get metadata of topic %s:%s", consumer.TopicName, err)
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

	consumer.SimpleConsumer2s = make(map[int32]*SimpleConsumer2)

	for _, topicMetadatas := range metadataResponse.TopicMetadatas {
		topicName := topicMetadatas.TopicName
		for _, partitionMetadataInfo := range topicMetadatas.PartitionMetadatas {
			partitionID := int32(partitionMetadataInfo.PartitionId)
			leaderID := partitionMetadataInfo.Leader
			offset := findOffset(topicName, partitionID, offsetsResponses)
			if _, ok := consumer.SimpleConsumer2s[leaderID]; ok {
				consumer.SimpleConsumer2s[leaderID].addPartition(partitionID, offset)
			} else {
				simpleConsumer2 := &SimpleConsumer2{
					Partitions:  make(map[int32]int64),
					Brokers:     consumer.Brokers,
					TopicName:   topicName,
					ClientID:    consumer.ClientID,
					MaxBytes:    consumer.MaxBytes,
					MaxWaitTime: consumer.MaxWaitTime,
					MinBytes:    consumer.MinBytes,
				}
				simpleConsumer2.addPartition(partitionID, offset)

				consumer.SimpleConsumer2s[leaderID] = simpleConsumer2
			}
		}
	}

	messages := make(chan *FullMessage, 10)
	for i, simpleConsumer2 := range consumer.SimpleConsumer2s {
		glog.V(10).Infof("SimpleConsumer2 %d", i)
		simpleConsumer2.Consume(messages)
	}

	return messages, nil
}
