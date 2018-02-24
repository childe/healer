package healer

import (
	"sync"

	"github.com/golang/glog"
)

type SimpleProducer struct {
	clientID         string
	broker           *Broker
	topic            string
	partition        int32
	acks             int16
	requestTimeoutMS int32
	compressionType  string
	compressionValue int8
	retries          int
	batchSize        int
	messageMaxCount  int
	metadataMaxAgeMS int

	messageSetSize int
	messageSet     MessageSet

	mutex sync.Locker
}

func NewSimpleProducer(topic string, partition int32, config map[string]interface{}) *SimpleProducer {
	p := &SimpleProducer{
		clientID:         "healer",
		topic:            topic,
		partition:        partition,
		acks:             1,
		requestTimeoutMS: 30000,
		compressionType:  "none",
		retries:          0,
		batchSize:        16384,
		messageMaxCount:  10,
		metadataMaxAgeMS: 300000,

		mutex: &sync.Mutex{},
	}

	if v, ok := config["compression.type"]; ok {
		compressionType := v.(string)
		if compressionType == "none" {
			p.compressionType = compressionType
			p.compressionValue = COMPRESSION_NONE
		} else if compressionType == "gzip" {
			p.compressionType = compressionType
			p.compressionValue = COMPRESSION_GZIP
		} else if compressionType == "snappy" {
			p.compressionType = compressionType
			p.compressionValue = COMPRESSION_SNAPPY
		} else if compressionType == "lz4" {
			p.compressionType = compressionType
			p.compressionValue = COMPRESSION_LZ4
		} else {
			glog.Fatalf("unknown compression type:%s", compressionType)
		}
	}

	if v, ok := config["message.max.count"]; ok {
		p.messageMaxCount = v.(int)
		if p.messageMaxCount <= 0 {
			glog.Fatal("messageMaxCount must > 0")
		}
	}
	p.messageSet = make([]*Message, p.messageMaxCount)

	// get partition leader
	var brokerList string
	if v, ok := config["bootstrap.servers"]; ok {
		brokerList = v.(string)
	} else {
		glog.Error("bootstrap.servers must be set")
		return nil
	}
	brokers, err := NewBrokers(brokerList, p.clientID, 60000, 30000)
	if err != nil {
		glog.Errorf("init brokers error:%s", err)
		return nil
	}

	leaderID, err := brokers.findLeader(p.clientID, p.topic, p.partition)
	if err != nil {
		glog.Errorf("could not get leader of topic %s[%d]:%s", p, topic, p.partition, err)
		return nil
	} else {
		glog.V(10).Infof("leader ID of [%s][%d] is %d", p.topic, p.partition, leaderID)
	}

	// TODO
	p.broker, err = brokers.NewBroker(leaderID)
	if err != nil {
		glog.Errorf("create broker error:%s", err)
		return nil
	} else {
		glog.V(5).Infof("leader broker %s", p.broker.GetAddress())
	}
	return p
}

func (simpleProducer *SimpleProducer) AddMessage(key []byte, value []byte) {
	message := &Message{
		Offset:      0,
		MessageSize: 0, // compute in message encode

		Crc:        0, // compute in message encode
		Attributes: 0 | simpleProducer.compressionValue,
		MagicByte:  0,
		Key:        key,
		Value:      value,
	}
	simpleProducer.messageSet[simpleProducer.messageSetSize] = message
	simpleProducer.messageSetSize++
	// TODO lock
	if simpleProducer.messageSetSize >= simpleProducer.messageMaxCount {
		// TODO copy and clean and emit?
		simpleProducer.Emit()
	}
}

func (simpleProducer *SimpleProducer) Emit() {
	simpleProducer.mutex.Lock()
	defer simpleProducer.mutex.Unlock()

	messageSet := simpleProducer.messageSet[:simpleProducer.messageSetSize]
	simpleProducer.messageSetSize = 0
	simpleProducer.messageSet = make([]*Message, simpleProducer.messageMaxCount)
	simpleProducer.emit(messageSet)
}

func (simpleProducer *SimpleProducer) emit(messageSet MessageSet) {
	produceRequest := &ProduceRequest{
		RequiredAcks: simpleProducer.acks,
		Timeout:      simpleProducer.requestTimeoutMS,
	}
	produceRequest.RequestHeader = &RequestHeader{
		ApiKey:     API_ProduceRequest,
		ApiVersion: 0,
		ClientId:   simpleProducer.clientID,
	}

	produceRequest.TopicBlocks = make([]struct {
		TopicName      string
		PartitonBlocks []struct {
			Partition      int32
			MessageSetSize int32
			MessageSet     MessageSet
		}
	}, 1)
	produceRequest.TopicBlocks[0].TopicName = simpleProducer.topic
	produceRequest.TopicBlocks[0].PartitonBlocks = make([]struct {
		Partition      int32
		MessageSetSize int32
		MessageSet     MessageSet
	}, 1)

	produceRequest.TopicBlocks[0].PartitonBlocks[0].Partition = simpleProducer.partition
	produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSetSize = int32(len(messageSet))
	//produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSet = []*Message{}
	produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSet = messageSet

	response, err := simpleProducer.broker.Request(produceRequest)
	glog.Info(response)
	glog.Info(err)
}
