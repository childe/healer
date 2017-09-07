package healer

import "github.com/golang/glog"

// SimpleConsumer instance is built to consume messages from kafka broker
type SimpleConsumer struct {
	ClientID    string
	Brokers     *Brokers
	BrokerList  string
	TopicName   string
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
	MaxWaitTime int32
	MinBytes    int32
}

func NewSimpleConsumer(brokers *Brokers) *SimpleConsumer {
	return nil
}

func (simpleConsumer *SimpleConsumer) Consume(offset int64) (chan *FullMessage, error) {
	leaderID, err := simpleConsumer.Brokers.findLeader(simpleConsumer.TopicName, simpleConsumer.Partition)
	if err != nil {
		//TODO NO fatal but return error
		glog.Fatal("could not get leader of topic %s:%s", simpleConsumer.TopicName, err)
	} else {
		glog.V(10).Infof("leader ID of [%s][%d] is %d", simpleConsumer.TopicName, simpleConsumer.Partition, leaderID)
	}

	var leaderBroker *Broker
	var ok bool
	if leaderBroker, ok = simpleConsumer.Brokers.brokers[leaderID]; !ok {
		//TODO NO fatal but return error
		glog.Fatal("could not get broker %d. maybe should refresh metadata.", leaderID)
	} else {
		glog.V(10).Infof("got leader broker %s with id %d", leaderBroker.address, leaderID)
	}

	var correlationID int32 = 0
	var messages chan *FullMessage = make(chan *FullMessage, 10)
	go func(chan *FullMessage) {
		for {
			correlationID++
			glog.V(10).Infof("correlationID: %d", correlationID)
			fetchRequest := NewFetchRequest(correlationID, simpleConsumer.ClientID, simpleConsumer.MaxWaitTime, simpleConsumer.MinBytes)
			fetchRequest.addPartition(simpleConsumer.TopicName, simpleConsumer.Partition, offset, simpleConsumer.MaxBytes)

			buffers := make(chan []byte, 10)
			innerMessages := make(chan *FullMessage, 10)
			go leaderBroker.requestFetchStreamingly(fetchRequest, buffers)
			fetchResponseStreamDecoder := FetchResponseStreamDecoder{
				totalLength: 0,
				offset:      0,
				length:      0,
				buffers:     buffers,
				messages:    innerMessages,
				more:        true,
			}
			go fetchResponseStreamDecoder.consumeFetchResponse()
			for {
				message, more := <-innerMessages
				if more {
					//glog.V(10).Infof("more message: %d %s", message.Offset, string(message.Value))
					offset = message.Message.Offset + 1
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

	return messages, nil
}
