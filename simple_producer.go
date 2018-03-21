package healer

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
)

type SimpleProducer struct {
	config *ProducerConfig

	broker    *Broker
	topic     string
	partition int32

	messageSetSize int
	messageSet     MessageSet

	mutex sync.Locker

	compressionValue int8
	compressor       Compressor
}

func NewSimpleProducer(topic string, partition int32, config *ProducerConfig) *SimpleProducer {
	err := config.checkValid()
	if err != nil {
		glog.Errorf("config error:%s", err)
		return nil
	}

	p := &SimpleProducer{
		config:    config,
		topic:     topic,
		partition: partition,

		mutex: &sync.Mutex{},
	}

	switch config.CompressionType {
	case "none":
		p.compressionValue = COMPRESSION_NONE
	case "gzip":
		p.compressionValue = COMPRESSION_GZIP
	case "snappy":
		p.compressionValue = COMPRESSION_SNAPPY
	case "lz4":
		p.compressionValue = COMPRESSION_LZ4
	}
	p.compressor = NewCompressor(config.CompressionType)

	if p.compressor == nil {
		glog.Error("could not build compressor for simple_producer")
		return nil
	}

	p.messageSet = make([]*Message, config.MessageMaxCount)

	// get partition leader
	brokers, err := NewBrokers(config.BootstrapServers, config.ClientID, 60000, 30000)
	if err != nil {
		glog.Errorf("init brokers error:%s", err)
		return nil
	}

	leaderID, err := brokers.findLeader(config.ClientID, p.topic, p.partition)
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

	// TODO wait to the next ticker to see if messageSet changes
	go func() {
		for range time.NewTicker(time.Duration(config.FlushIntervalMS) * time.Millisecond).C {
			p.mutex.Lock()
			if p.messageSetSize == 0 {
				p.mutex.Unlock()
				continue
			}

			messageSet := p.messageSet[:p.messageSetSize]
			p.messageSetSize = 0
			p.messageSet = make([]*Message, config.MessageMaxCount)
			p.mutex.Unlock()

			p.flush(messageSet)
		}
	}()

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
	if simpleProducer.messageSetSize >= simpleProducer.config.MessageMaxCount {
		// TODO copy and clean and flush?
		simpleProducer.Flush()
	}
	return nil
}

func (simpleProducer *SimpleProducer) Flush() error {
	simpleProducer.mutex.Lock()

	if simpleProducer.messageSetSize == 0 {
		return nil
	}

	messageSet := simpleProducer.messageSet[:simpleProducer.messageSetSize]
	simpleProducer.messageSetSize = 0
	simpleProducer.messageSet = make([]*Message, simpleProducer.config.MessageMaxCount)
	simpleProducer.mutex.Unlock()

	return simpleProducer.flush(messageSet)
}

func (simpleProducer *SimpleProducer) flush(messageSet MessageSet) error {
	produceRequest := &ProduceRequest{
		RequiredAcks: simpleProducer.config.Acks,
		Timeout:      simpleProducer.config.RequestTimeoutMS,
	}
	produceRequest.RequestHeader = &RequestHeader{
		ApiKey:     API_ProduceRequest,
		ApiVersion: 0,
		ClientId:   simpleProducer.config.ClientID,
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
