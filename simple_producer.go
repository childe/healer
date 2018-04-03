package healer

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
)

var SimpleProducerClosedError = errors.New("simple producer has been closed and failed to open")

type SimpleProducer struct {
	config *ProducerConfig

	broker    *Broker
	topic     string
	partition int32
	closed    bool

	messageSetSize int
	messageSet     MessageSet

	mutex sync.Locker
	timer *time.Timer

	compressionValue int8
	compressor       Compressor
}

func (p *SimpleProducer) createBroker() (*Broker, error) {
	brokers, err := NewBrokers(p.config.BootstrapServers, p.config.ClientID, DefaultBrokerConfig())
	if err != nil {
		glog.Errorf("init brokers error: %s", err)
		return nil, err
	}

	leaderID, err := brokers.findLeader(p.config.ClientID, p.topic, p.partition)
	if err != nil {
		glog.Errorf("could not get leader of topic %s[%d]: %s", p.topic, p.partition, err)
		return nil, err
	} else {
		glog.V(10).Infof("leader ID of [%s][%d] is %d", p.topic, p.partition, leaderID)
	}

	broker, err := brokers.NewBroker(leaderID)
	if err != nil {
		glog.Errorf("create broker error: %s", err)
		return nil, err
	} else {
		glog.V(5).Infof("leader broker %s", broker.GetAddress())
	}

	brokers.Close()

	return broker, err
}

func NewSimpleProducer(topic string, partition int32, config *ProducerConfig) *SimpleProducer {
	err := config.checkValid()
	if err != nil {
		glog.Errorf("config error: %s", err)
		return nil
	}

	p := &SimpleProducer{
		config:    config,
		topic:     topic,
		partition: partition,
		closed:    false,

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

	p.broker, err = p.createBroker()
	if err != nil {
		glog.Errorf("create producer broker error: %s", err)
		return nil
	}

	p.timer = time.NewTimer(time.Duration(config.ConnectionsMaxIdleMS) * time.Millisecond)
	go func() {
		<-p.timer.C
		p.Close()
	}()

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

func (p *SimpleProducer) ensureOpen() bool {
	var err error
	if p.closed == false {
		return true
	}

	p.broker, err = p.createBroker()
	if err != nil {
		glog.Error("create producer broker error: %s", err)
		return false
	}

	p.closed = false

	p.timer = time.NewTimer(time.Duration(p.config.ConnectionsMaxIdleMS) * time.Millisecond)
	go func() {
		<-p.timer.C
		p.Close()
	}()

	return true
}

func (p *SimpleProducer) AddMessage(key []byte, value []byte) error {
	if p.ensureOpen() == false {
		return SimpleProducerClosedError
	}
	message := &Message{
		Offset:      0,
		MessageSize: 0, // compute in message encode

		Crc:        0,    // compute in message encode
		Attributes: 0x00, // compress in upper message set level
		MagicByte:  1,
		Key:        key,
		Value:      value,
	}
	p.messageSet[p.messageSetSize] = message
	p.messageSetSize++
	if p.messageSetSize >= p.config.MessageMaxCount {
		p.Flush()
	}
	return nil
}

func (p *SimpleProducer) Flush() error {
	p.mutex.Lock()

	if p.messageSetSize == 0 {
		p.mutex.Unlock()
		return nil
	}

	messageSet := p.messageSet[:p.messageSetSize]
	p.messageSetSize = 0
	p.messageSet = make([]*Message, p.config.MessageMaxCount)
	p.mutex.Unlock()

	if !p.timer.Stop() {
		<-p.timer.C
	}
	p.timer.Reset(time.Duration(p.config.ConnectionsMaxIdleMS) * time.Millisecond)

	return p.flush(messageSet)
}

func (p *SimpleProducer) flush(messageSet MessageSet) error {
	produceRequest := &ProduceRequest{
		RequiredAcks: p.config.Acks,
		Timeout:      p.config.RequestTimeoutMS,
	}
	produceRequest.RequestHeader = &RequestHeader{
		ApiKey:     API_ProduceRequest,
		ApiVersion: 0,
		ClientId:   p.config.ClientID,
	}

	produceRequest.TopicBlocks = make([]struct {
		TopicName      string
		PartitonBlocks []struct {
			Partition      int32
			MessageSetSize int32
			MessageSet     MessageSet
		}
	}, 1)
	produceRequest.TopicBlocks[0].TopicName = p.topic
	produceRequest.TopicBlocks[0].PartitonBlocks = make([]struct {
		Partition      int32
		MessageSetSize int32
		MessageSet     MessageSet
	}, 1)

	if p.compressionValue != 0 {
		value := make([]byte, messageSet.Length())
		messageSet.Encode(value, 0)
		compressed_value, err := p.compressor.Compress(value)
		if err != nil {
			return fmt.Errorf("compress messageset error:%s", err)
		}
		var message *Message = &Message{
			Offset:      0,
			MessageSize: 0, // compute in message encode

			Crc:        0, // compute in message encode
			Attributes: 0x00 | p.compressionValue,
			MagicByte:  1,
			Key:        nil,
			Value:      compressed_value,
		}
		messageSet = []*Message{message}
	}
	produceRequest.TopicBlocks[0].PartitonBlocks[0].Partition = p.partition
	produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSetSize = int32(len(messageSet))
	produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSet = messageSet

	responseBuf, err := p.broker.Request(produceRequest)
	if err != nil {
		return err
	}
	response, err := NewProduceResponse(responseBuf)
	if glog.V(10) {
		b, _ := json.Marshal(response)
		glog.Infof("produces response: %s", b)
	}
	return err
}

func (p *SimpleProducer) Close() {
	glog.Info("flush before SimpleProducer is closed")
	p.Flush()
	glog.Info("SimpleProducer closing")
	p.closed = true
	p.broker.Close()
}
