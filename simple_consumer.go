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
	// TODO
	return nil
}

func (sc *SimpleConsumer) getOffset(fromBeginning bool) (int64, error) {
	var time int64
	if fromBeginning {
		time = -2
	} else {
		time = -1
	}
	offsetsResponses, err := sc.Brokers.RequestOffsets(sc.ClientID, sc.TopicName, sc.Partition, time, 1)
	if err != nil {
		return -1, err
	}

	return int64(offsetsResponses[0].Info[sc.TopicName][0].Offset[0]), nil
}

func (simpleConsumer *SimpleConsumer) Consume(offset int64, messageChan chan *FullMessage) (chan *FullMessage, error) {
	var (
		err          error
		leaderBroker *Broker
		ok           bool
		leaderID     int32
	)
	leaderID, err = simpleConsumer.Brokers.findLeader(simpleConsumer.ClientID, simpleConsumer.TopicName, simpleConsumer.Partition)
	if err != nil {
		//TODO NO fatal but return error
		glog.Fatal("could not get leader of topic %s:%s", simpleConsumer.TopicName, err)
	} else {
		glog.V(10).Infof("leader ID of [%s][%d] is %d", simpleConsumer.TopicName, simpleConsumer.Partition, leaderID)
	}

	if leaderBroker, ok = simpleConsumer.Brokers.brokers[leaderID]; !ok {
		//TODO NO fatal but return error
		glog.Fatal("could not get broker %d. maybe should refresh metadata.", leaderID)
	} else {
		glog.V(10).Infof("got leader broker %s with id %d", leaderBroker.address, leaderID)
	}

	// TODO brokers may not need? it should just reserve some meta info
	leaderBroker, err = NewBroker(leaderBroker.address, leaderBroker.nodeID, 30, 60)
	if err != nil {
		glog.Fatal(err)
	}

	if offset == -1 {
		offset, err = simpleConsumer.getOffset(false)
	} else if offset == -2 {
		offset, err = simpleConsumer.getOffset(true)
	}
	if err != nil {
		glog.Fatalf("could not get offset %s[%d]:%s", simpleConsumer.TopicName, simpleConsumer.Partition, err)
	}

	var messages chan *FullMessage
	if messageChan == nil {
		messages = make(chan *FullMessage, 10)
	} else {
		messages = messageChan
	}
	go func(chan *FullMessage) {
		for {
			// TODO set CorrelationID to 0 firstly and then set by broker
			fetchRequest := NewFetchRequest(0, simpleConsumer.ClientID, simpleConsumer.MaxWaitTime, simpleConsumer.MinBytes)
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
