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

// Consume consume  messages from kafka broker and send them to channels
// TODO goroutine and return another chan? the return could control when to stop
func (simpleConsumer *SimpleConsumer) Consume(messages chan *Message) {
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

	correlationID := int32(0)
	fetchRequest := NewFetchRequest(correlationID, simpleConsumer.ClientID, simpleConsumer.MaxWaitTime, simpleConsumer.MinBytes)
	fetchRequest.addPartition(simpleConsumer.TopicName, simpleConsumer.Partition, simpleConsumer.FetchOffset, simpleConsumer.MaxBytes)

	// TODO when stop??
	for {
		fetchResponse, err := leaderBroker.requestFetch(fetchRequest)
		if err != nil {
			glog.Errorf("request fetch error: %s", err)
			continue
		}

		for _, fetchResponsePiece := range fetchResponse.Responses {
			for _, topicData := range fetchResponsePiece.PartitionResponses {
				if topicData.ErrorCode == 0 {
					for _, message := range topicData.MessageSet {
						fetchRequest.Topics[simpleConsumer.TopicName][0].FetchOffset = message.Offset + 1
						messages <- message
					}
				} else if topicData.ErrorCode == -1 {
					glog.Info(AllError[0].Error())
				} else {
					glog.Info(AllError[topicData.ErrorCode].Error())
				}
			}
		}
		correlationID++
		fetchRequest.RequestHeader.CorrelationId = correlationID
	}
}
