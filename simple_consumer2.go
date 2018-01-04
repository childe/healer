package healer

import "github.com/golang/glog"

// SimpleConsumer2 instance is built to consume messages of one or more partitions of one topic
// this is used for consumer

type SimpleConsumer2 struct {
	ClientID    string
	Brokers     *Brokers
	BrokerList  string
	TopicName   string
	Partitions  map[int32]int64
	MaxBytes    int32
	MaxWaitTime int32
	MinBytes    int32

	LeaderID int32
}

func NewSimpleConsumer2(brokers *Brokers) *SimpleConsumer2 {
	return nil
}

func (simpleConsumer2 *SimpleConsumer2) addPartition(partitionID int32, offset int64) {
	simpleConsumer2.Partitions[partitionID] = offset
}

func (simpleConsumer2 *SimpleConsumer2) Consume(messages chan *FullMessage) error {
	var (
		leaderID     int32 = -1
		leaderBroker *Broker
		ok           bool
	)

	for partition, _ := range simpleConsumer2.Partitions {
		_leaderID, err := simpleConsumer2.Brokers.findLeader(simpleConsumer2.ClientID, simpleConsumer2.TopicName, partition)
		if err != nil {
			//TODO NO fatal but return error
			glog.Fatal("could not get leader of %s[%d]:%s", simpleConsumer2.TopicName, partition, err)
		} else {
			glog.V(10).Infof("leader ID of [%s][%d] is %d", simpleConsumer2.TopicName, partition, _leaderID)
		}

		if leaderID == -1 {
			leaderID = _leaderID
			continue
		}

		if leaderID != _leaderID {
			glog.Fatal("all partitions should have the same leader")
		}
	}

	if leaderBroker, ok = simpleConsumer2.Brokers.brokers[leaderID]; !ok {
		//TODO NO fatal but return error
		glog.Fatal("could not get broker %d. maybe should refresh metadata.", leaderID)
	} else {
		glog.V(10).Infof("got leader broker %s with id %d", leaderBroker.address, leaderID)
	}

	var correlationID uint32 = 0
	go func(chan *FullMessage) {
		for {
			correlationID++
			glog.V(10).Infof("correlationID: %d", correlationID)
			fetchRequest := NewFetchRequest(correlationID, simpleConsumer2.ClientID, simpleConsumer2.MaxWaitTime, simpleConsumer2.MinBytes)
			for partitionID, offset := range simpleConsumer2.Partitions {
				fetchRequest.addPartition(simpleConsumer2.TopicName, partitionID, offset, simpleConsumer2.MaxBytes)
			}

			buffers := make(chan []byte, 10)
			innerMessages := make(chan *FullMessage, 10)
			go leaderBroker.requestFetchStreamingly(fetchRequest, buffers)
			streamDecoder := FetchResponseStreamDecoder{
				totalLength: 0,
				length:      0,
				buffers:     buffers,
				messages:    innerMessages,
				more:        true,
			}
			go streamDecoder.consumeFetchResponse()
			for {
				message, more := <-innerMessages
				if more {
					//glog.V(10).Infof("more message: %d %s", message.Offset, string(message.Value))
					simpleConsumer2.Partitions[message.PartitionID] = message.Message.Offset + 1
					messages <- message
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
		}
	}(messages)

	return nil
}
