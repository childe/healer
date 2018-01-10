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

	leaderBroker *Broker

	stop          bool
	fromBeginning bool

	BelongTO *GroupConsumer
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

func (simpleConsumer *SimpleConsumer) Stop() {
	simpleConsumer.stop = true
}

// if offset is -1 or -2, first check if has previous offset committed. it will continue if it exists
func (simpleConsumer *SimpleConsumer) Consume(offset int64, messageChan chan *FullMessage) (chan *FullMessage, error) {
	var (
		err      error
		leaderID int32
	)

	simpleConsumer.stop = false

	leaderID, err = simpleConsumer.Brokers.findLeader(simpleConsumer.ClientID, simpleConsumer.TopicName, simpleConsumer.Partition)
	if err != nil {
		//TODO NO fatal but return error
		glog.Fatal("could not get leader of topic %s:%s", simpleConsumer.TopicName, err)
	} else {
		glog.V(10).Infof("leader ID of [%s][%d] is %d", simpleConsumer.TopicName, simpleConsumer.Partition, leaderID)
	}

	// TODO
	simpleConsumer.leaderBroker, err = simpleConsumer.Brokers.NewBroker(leaderID)
	if err != nil {
		//TODO NO fatal but return error
		glog.Fatalf("could not get broker %d. maybe should refresh metadata.", leaderID)
	} else {
		glog.V(10).Infof("got leader broker %s with id %d", simpleConsumer.leaderBroker.address, leaderID)
	}

	glog.V(5).Infof("[%s][%d] offset :%d", simpleConsumer.TopicName, simpleConsumer.Partition, offset)

	if simpleConsumer.BelongTO != nil {
		if offset == -1 || offset == -2 {
			r := NewOffsetFetchRequest(1, simpleConsumer.ClientID, simpleConsumer.BelongTO.groupID)
			r.AddPartiton(simpleConsumer.TopicName, simpleConsumer.Partition)

			response, err := simpleConsumer.Brokers.Request(r)
			if err != nil {
				glog.Fatal("request fetch offset for [%s][%d] error:%s", simpleConsumer.TopicName, simpleConsumer.Partition, err)
			}

			res, err := NewOffsetFetchResponse(response)
			if res == nil {
				glog.Fatalf("decode offset fetch response error:%s", err)
			}

			for _, t := range res.Topics {
				if t.Topic != simpleConsumer.TopicName {
					continue
				}
				for _, p := range t.Partitions {
					if int32(p.PartitionID) == simpleConsumer.Partition {
						offset = p.Offset
						break
					}
				}
			}
		}

		glog.Infof("consume [%s][%d] from %d", simpleConsumer.TopicName, simpleConsumer.Partition, offset)
	}

	// offset not fetched from OffsetFetchRequest
	if offset == -1 {
		simpleConsumer.fromBeginning = false
		offset, err = simpleConsumer.getOffset(simpleConsumer.fromBeginning)
	} else if offset == -2 {
		simpleConsumer.fromBeginning = true
		offset, err = simpleConsumer.getOffset(simpleConsumer.fromBeginning)
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
	go func(messages chan *FullMessage) {
		for simpleConsumer.stop == false {
			// TODO set CorrelationID to 0 firstly and then set by broker
			fetchRequest := NewFetchRequest(0, simpleConsumer.ClientID, simpleConsumer.MaxWaitTime, simpleConsumer.MinBytes)
			fetchRequest.addPartition(simpleConsumer.TopicName, simpleConsumer.Partition, offset, simpleConsumer.MaxBytes)

			buffers := make(chan []byte, 10)
			innerMessages := make(chan *FullMessage, 10)
			go simpleConsumer.leaderBroker.requestFetchStreamingly(fetchRequest, buffers)
			fetchResponseStreamDecoder := FetchResponseStreamDecoder{
				totalLength: 0,
				length:      0,
				buffers:     buffers,
				messages:    innerMessages,
				more:        true,
			}
			go fetchResponseStreamDecoder.consumeFetchResponse()
			for simpleConsumer.stop == false {
				message, more := <-innerMessages
				if more {
					if message.Error != nil {
						glog.Infof("consumer %s[%d] error:%s", simpleConsumer.TopicName, simpleConsumer.Partition, message.Error)
						if message.Error == AllError[1] {
							offset, err = simpleConsumer.getOffset(simpleConsumer.fromBeginning)
							if err != nil {
								glog.Infof("could not get %s[%d] offset:%s", simpleConsumer.TopicName, simpleConsumer.Partition, message.Error)
							}
						}
					} else {
						offset = message.Message.Offset + 1
						messages <- message
					}
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

			if simpleConsumer.BelongTO != nil {
				simpleConsumer.BelongTO.CommitOffset(simpleConsumer.TopicName, simpleConsumer.Partition, offset)
			}
		}
		simpleConsumer.leaderBroker.Close()
	}(messages)

	return messages, nil
}
