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

	leader    *Broker
	topic     string
	partition int32
	closed    bool

	messageSet MessageSet

	messageSetMutex sync.Locker
	flushMutex      sync.Locker
	timer           *time.Timer

	compressionValue int8
	compressor       Compressor
}

func (p *SimpleProducer) createLeader() (*Broker, error) {
	brokerConfig := getBrokerConfigFromProducerConfig(p.config)
	brokers, err := NewBrokersWithConfig(p.config.BootstrapServers, brokerConfig)
	if err != nil {
		glog.Errorf("init brokers error: %s", err)
		return nil, err
	}
	defer brokers.Close()

	leaderID, err := brokers.findLeader(p.config.ClientID, p.topic, p.partition)
	if err != nil {
		glog.Errorf("could not get leader of topic %s[%d]: %s", p.topic, p.partition, err)
		return nil, err
	} else {
		glog.V(10).Infof("leader ID of [%s][%d] is %d", p.topic, p.partition, leaderID)
	}

	leader, err := brokers.NewBroker(leaderID)
	if err != nil {
		glog.Errorf("create leader error: %s", err)
		return nil, err
	} else {
		glog.V(5).Infof("leader broker %s", leader.GetAddress())
	}

	return leader, err
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
		closed:    true,

		messageSetMutex: &sync.Mutex{},
		flushMutex:      &sync.Mutex{},
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
	p.messageSet = p.messageSet[:0]

	p.ensureOpen()

	return p
}

func (p *SimpleProducer) ensureOpen() bool {
	var err error
	if p.closed == false {
		return true
	}

	p.leader, err = p.createLeader()
	if err != nil {
		glog.Errorf("create producer leader error: %s", err)
		return false
	}

	p.closed = false

	p.timer = time.NewTimer(time.Duration(p.config.ConnectionsMaxIdleMS) * time.Millisecond)
	go func() {
		<-p.timer.C
		p.Close()
	}()

	// TODO wait to the next ticker to see if messageSet changes
	go func() {
		for range time.NewTicker(time.Duration(p.config.FlushIntervalMS) * time.Millisecond).C {
			p.flushMutex.Lock()
			if p.closed {
				p.flushMutex.Unlock()
				return
			}
			p.Flush()
			p.flushMutex.Unlock()
		}
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
		MagicByte:  0,
		Key:        key,
		Value:      value,
	}
	p.messageSetMutex.Lock()
	p.messageSet = append(p.messageSet, message)
	p.messageSetMutex.Unlock()
	if len(p.messageSet) >= p.config.MessageMaxCount {
		p.Flush()
	}
	return nil
}

func (p *SimpleProducer) Flush() error {
	if len(p.messageSet) == 0 {
		return nil
	}

	p.messageSetMutex.Lock()
	messageSet := p.messageSet
	p.messageSet = make([]*Message, 0, p.config.MessageMaxCount)
	p.messageSetMutex.Unlock()

	// TODO should below code put between lock & unlock?
	// TODO reading from channel will take too long?
	if !p.timer.Stop() {
		<-p.timer.C
	}
	p.timer.Reset(time.Duration(p.config.ConnectionsMaxIdleMS) * time.Millisecond)

	return p.flush(messageSet)
}

func (p *SimpleProducer) flush(messageSet MessageSet) error {
	if len(messageSet) == 0 {
		return nil
	}

	if glog.V(5) {
		glog.Infof("produce %d messsages", len(messageSet))
	}

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
			MagicByte:  0,
			Key:        nil,
			Value:      compressed_value,
		}
		messageSet = []*Message{message}
	}
	produceRequest.TopicBlocks[0].PartitonBlocks[0].Partition = p.partition
	produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSetSize = int32(len(messageSet))
	produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSet = messageSet

	responseBuf, err := p.leader.Request(produceRequest)
	if err != nil {
		glog.Errorf("produce request error: %s", err)
		return err
	}
	response, err := NewProduceResponse(responseBuf)
	if glog.V(10) {
		b, _ := json.Marshal(response)
		glog.Infof("produces response: %s", b)
	}
	if err != nil {
		glog.Errorf("decode produce response error: %s", err)
	}
	return err
}

func (p *SimpleProducer) Close() {
	p.flushMutex.Lock()
	defer p.flushMutex.Unlock()

	glog.Info("flush before SimpleProducer is closed")
	p.Flush()
	glog.Info("SimpleProducer closing")
	p.closed = true
	p.leader.Close()
}
