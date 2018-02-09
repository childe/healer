package healer

import (
	"time"

	"github.com/golang/glog"
)

// SimpleConsumer instance is built to consume messages from kafka broker
type SimpleConsumer struct {
	ClientID             string
	Brokers              *Brokers
	BrokerList           string
	TopicName            string
	Partition            int32
	FetchOffset          int64
	MaxBytes             int32
	MaxWaitTime          int32
	MinBytes             int32
	AutoCommitIntervalMs int
	AutoCommit           bool
	CommitAfterFetch     bool
	OffsetsStorage       int // 0 zk, 1 kafka

	leaderBroker *Broker

	stop           bool
	fromBeginning  bool
	offset         int64
	offsetCommited int64

	BelongTO *GroupConsumer
}

func NewSimpleConsumer(brokers *Brokers) *SimpleConsumer {
	// TODO
	return nil
}

func (simpleConsumer *SimpleConsumer) getLeaderBroker() error {
	var (
		err      error
		leaderID int32
	)

	leaderID, err = simpleConsumer.Brokers.findLeader(simpleConsumer.ClientID, simpleConsumer.TopicName, simpleConsumer.Partition)
	if err != nil {
		//glog.Fatal("could not get leader of topic %s:%s", simpleConsumer.TopicName, err)
		return err
	} else {
		glog.V(10).Infof("leader ID of [%s][%d] is %d", simpleConsumer.TopicName, simpleConsumer.Partition, leaderID)
	}

	// TODO
	simpleConsumer.leaderBroker, err = simpleConsumer.Brokers.NewBroker(leaderID)
	if err != nil {
		return err
		//glog.Fatalf("could not get broker %d. maybe should refresh metadata.", leaderID)
	} else {
		glog.V(5).Infof("got leader broker %s with id %d", simpleConsumer.leaderBroker.address, leaderID)
	}
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

// if offset is -1 or -2, first check if has previous offset committed if its BelongTO is not nil
func (simpleConsumer *SimpleConsumer) Consume(offset int64, messageChan chan *FullMessage) (chan *FullMessage, error) {
	var err error

	simpleConsumer.stop = false
	simpleConsumer.offset = offset

	glog.V(5).Infof("[%s][%d] offset :%d", simpleConsumer.TopicName, simpleConsumer.Partition, simpleConsumer.offset)

	err = simpleConsumer.getLeaderBroker()
	// TODO pass error to caller?
	if err != nil {
		glog.Fatalf("could get leader broker:%s", err)
	}

	if simpleConsumer.BelongTO != nil && (simpleConsumer.offset == -1 || simpleConsumer.offset == -2) {
		var apiVersion uint16
		if simpleConsumer.OffsetsStorage == 0 {
			apiVersion = 0
		} else if simpleConsumer.OffsetsStorage == 1 {
			apiVersion = 1
		} else {
			glog.Fatalf("offsets.storage (%d) illegal", simpleConsumer.OffsetsStorage)
		}
		r := NewOffsetFetchRequest(apiVersion, simpleConsumer.ClientID, simpleConsumer.BelongTO.groupID)
		r.AddPartiton(simpleConsumer.TopicName, simpleConsumer.Partition)

		response, err := simpleConsumer.BelongTO.coordinator.Request(r)
		if err != nil {
			glog.Fatalf("request fetch offset for [%s][%d] error:%s", simpleConsumer.TopicName, simpleConsumer.Partition, err)
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
					simpleConsumer.offset = p.Offset
					break
				}
			}
		}
	}

	// offset not fetched from OffsetFetchRequest
	if simpleConsumer.offset == -1 {
		simpleConsumer.fromBeginning = false
		simpleConsumer.offset, err = simpleConsumer.getOffset(simpleConsumer.fromBeginning)
	} else if simpleConsumer.offset == -2 {
		simpleConsumer.fromBeginning = true
		simpleConsumer.offset, err = simpleConsumer.getOffset(simpleConsumer.fromBeginning)
	}
	if err != nil {
		glog.Fatalf("could not get offset %s[%d]:%s", simpleConsumer.TopicName, simpleConsumer.Partition, err)
	}
	glog.Infof("consume [%s][%d] from %d", simpleConsumer.TopicName, simpleConsumer.Partition, simpleConsumer.offset)

	var messages chan *FullMessage
	if messageChan == nil {
		messages = make(chan *FullMessage, 10)
	} else {
		messages = messageChan
	}

	if simpleConsumer.AutoCommit {
		ticker := time.NewTicker(time.Millisecond * time.Duration(simpleConsumer.AutoCommitIntervalMs))
		go func() {
			for range ticker.C {
				if simpleConsumer.stop {
					return
				}
				if simpleConsumer.offset != simpleConsumer.offsetCommited {
					simpleConsumer.BelongTO.CommitOffset(simpleConsumer.TopicName, simpleConsumer.Partition, simpleConsumer.offset)
					simpleConsumer.offsetCommited = simpleConsumer.offset
				}
			}
		}()
	}

	go func(messages chan *FullMessage) {
		defer func() {
			glog.V(10).Infof("simple consumer (%s) stop consuming", simpleConsumer.ClientID)
		}()
		for simpleConsumer.stop == false {
			// TODO set CorrelationID to 0 firstly and then set by broker
			fetchRequest := NewFetchRequest(simpleConsumer.ClientID, simpleConsumer.MaxWaitTime, simpleConsumer.MinBytes)
			fetchRequest.addPartition(simpleConsumer.TopicName, simpleConsumer.Partition, simpleConsumer.offset, simpleConsumer.MaxBytes)

			buffers := make(chan []byte, 10)
			innerMessages := make(chan *FullMessage, 10)
			go func() {
				err := simpleConsumer.leaderBroker.requestFetchStreamingly(fetchRequest, buffers)
				if err != nil {
					glog.Errorf("fetch error:%s", err)
				}
			}()

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
							simpleConsumer.offset, err = simpleConsumer.getOffset(simpleConsumer.fromBeginning)
							if err != nil {
								glog.Errorf("could not get %s[%d] offset:%s", simpleConsumer.TopicName, simpleConsumer.Partition, message.Error)
							}
						} else if message.Error == AllError[6] {
							err = simpleConsumer.getLeaderBroker()
							if err != nil {
								// TODO pass errro to caller?
								glog.Fatalf("could get leader broker:%s", err)
							}
						}
					} else {
						simpleConsumer.offset = message.Message.Offset + 1
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

			if simpleConsumer.BelongTO != nil && simpleConsumer.CommitAfterFetch && simpleConsumer.offset != simpleConsumer.offsetCommited {
				simpleConsumer.BelongTO.CommitOffset(simpleConsumer.TopicName, simpleConsumer.Partition, simpleConsumer.offset)
				simpleConsumer.offsetCommited = simpleConsumer.offset
			}
		}
		simpleConsumer.leaderBroker.Close()
	}(messages)

	return messages, nil
}
