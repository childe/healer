package healer

import (
	"fmt"
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

	compressor Compressor
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

	var compressionType string
	if v, ok := config["compression.type"]; ok {
		compressionType = v.(string)
	} else {
		compressionType = "none"
	}
	p.compressionType = compressionType
	switch compressionType {
	case "none":
		p.compressionValue = COMPRESSION_NONE
	case "gzip":
		p.compressionValue = COMPRESSION_GZIP
	case "snappy":
		p.compressionValue = COMPRESSION_SNAPPY
	case "lz4":
		p.compressionValue = COMPRESSION_LZ4
	default:
		glog.Errorf("unknown compression type:%s", compressionType)
		return nil
	}
	p.compressor = NewCompressor(compressionType)

	if p.compressor == nil {
		glog.Error("could not build compressor for simple_producer")
		return nil
	}

	if v, ok := config["message.max.count"]; ok {
		p.messageMaxCount = v.(int)
		if p.messageMaxCount <= 0 {
			glog.Error("message.max.count must > 0")
			return nil
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
		glog.Errorf("could not get leader of topic %s[%d]:%s", p.topic, p.partition, err)
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

func (simpleProducer *SimpleProducer) AddMessage(key []byte, value []byte) error {
	message := &Message{
		Offset:      0,
		MessageSize: 0, // compute in message encode

		Crc:        0,    // compute in message encode
		Attributes: 0x00, // compress in upper message set level
		MagicByte:  1,
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
	return nil
}

func (simpleProducer *SimpleProducer) Emit() error {
	simpleProducer.mutex.Lock()
	defer simpleProducer.mutex.Unlock()

	messageSet := simpleProducer.messageSet[:simpleProducer.messageSetSize]
	simpleProducer.messageSetSize = 0
	simpleProducer.messageSet = make([]*Message, simpleProducer.messageMaxCount)
	return simpleProducer.emit(messageSet)
}

func (simpleProducer *SimpleProducer) emit(messageSet MessageSet) error {
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

	if simpleProducer.compressionValue != 0 {
		value := make([]byte, messageSet.Length())
		messageSet.Encode(value, 0)
		compressed_value, err := simpleProducer.compressor.Compress(value)
		if err != nil {
			return fmt.Errorf("compress messageset error:%s", err)
		}
		var message *Message = &Message{
			Offset:      0,
			MessageSize: 0, // compute in message encode

			Crc:        0, // compute in message encode
			Attributes: 0x00 | simpleProducer.compressionValue,
			MagicByte:  1,
			Key:        nil,
			Value:      compressed_value,
		}
		messageSet = []*Message{message}
	}
	produceRequest.TopicBlocks[0].PartitonBlocks[0].Partition = simpleProducer.partition
	produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSetSize = int32(len(messageSet))
	produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSet = messageSet

	response, err := simpleProducer.broker.Request(produceRequest)
	glog.Info(response)
	glog.Info(err)
	return err
}
