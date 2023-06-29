package healer

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
)

// ErrProducerClosed is returned when adding message while producer is closed
var ErrProducerClosed = fmt.Errorf("producer closed")

// SimpleProducer is a simple producer that send message to one topic and partitions with same leader
type SimpleProducer struct {
	config *ProducerConfig

	brokers *Brokers
	leader  *Broker

	topic     string
	partition int32

	messageSet MessageSet

	lock sync.Mutex

	closeChan chan struct{}
	closed    bool

	compressionValue int8
	compressor       Compressor
}

func (p *SimpleProducer) createLeader() (*Broker, error) {
	if p.brokers == nil {
		brokerConfig := getBrokerConfigFromProducerConfig(p.config)
		brokers, err := NewBrokersWithConfig(p.config.BootstrapServers, brokerConfig)
		if err != nil {
			glog.Errorf("init brokers error: %s", err)
			return nil, err
		}
		p.brokers = brokers
	}

	leaderID, err := p.brokers.findLeader(p.config.ClientID, p.topic, p.partition)
	if err != nil {
		glog.Errorf("could not get leader of topic %s[%d]: %s", p.topic, p.partition, err)
		return nil, err
	} else {
		glog.V(10).Infof("leader ID of [%s][%d] is %d", p.topic, p.partition, leaderID)
	}

	leader, err := p.brokers.NewBroker(leaderID)
	if err != nil {
		glog.Errorf("create leader error: %s", err)
		return nil, err
	} else {
		glog.V(5).Infof("leader broker %s", leader.GetAddress())
	}

	return leader, err
}

func NewSimpleProducer(topic string, partition int32, config *ProducerConfig, leader *Broker) *SimpleProducer {
	err := config.checkValid()
	if err != nil {
		glog.Errorf("config error: %s", err)
		return nil
	}

	p := &SimpleProducer{
		config:    config,
		topic:     topic,
		partition: partition,

		closeChan: make(chan struct{}, 0),
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

	p.messageSet = make([]*Message, 0, config.MessageMaxCount)

	if leader != nil {
		p.leader = leader
	} else {
		p.leader, err = p.createLeader()
	}
	if err != nil {
		glog.Errorf("create producer leader error: %s", err)
		return nil
	}

	// TODO wait to the next ticker to see if messageSet changes
	go func() {
		ticker := time.NewTicker(time.Duration(p.config.FlushIntervalMS) * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				p.Flush()
			case <-p.closeChan:
				return
			}
		}
	}()

	return p
}

// AddMessage add message to message set. If message set is full, send it to kafka synchronously
func (p *SimpleProducer) AddMessage(key []byte, value []byte) error {
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	message := &Message{
		Offset:      0,
		MessageSize: 0, // compute in message encode

		Crc:        0,    // compute in message encode
		Attributes: 0x00, // compress in upper message set level
		MagicByte:  int8(p.config.HealerMagicByte),
		Key:        key,
		Value:      valueCopy,
	}
	if p.config.HealerMagicByte == 1 {
		message.Timestamp = uint64(time.Now().UnixMilli())
	}
	p.lock.Lock()

	if p.closed {
		p.lock.Unlock()
		return ErrProducerClosed
	}
	p.messageSet = append(p.messageSet, message)
	if len(p.messageSet) >= p.config.MessageMaxCount {
		messageSet := p.messageSet
		p.messageSet = make([]*Message, 0, p.config.MessageMaxCount)
		p.lock.Unlock()
		p.flush(messageSet)
	} else {
		p.lock.Unlock()
	}
	return nil
}

// Flush send all messages to kafka
func (p *SimpleProducer) Flush() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.messageSet) > 0 {
		messageSet := p.messageSet
		p.messageSet = make([]*Message, 0, p.config.MessageMaxCount)
		return p.flush(messageSet)
	}
	return nil
}

func (p *SimpleProducer) flush(messageSet MessageSet) error {
	if glog.V(5) {
		glog.Infof("produce %d messsages to %s-%d", len(messageSet), p.topic, p.partition)
	}

	produceRequest := &ProduceRequest{
		RequiredAcks: p.config.Acks,
		Timeout:      p.config.RequestTimeoutMS,
	}
	produceRequest.RequestHeader = &RequestHeader{
		APIKey:     API_ProduceRequest,
		APIVersion: 0,
		ClientID:   p.config.ClientID,
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
		// FIXME: compressed message size if larger than before?
		value := make([]byte, messageSet.Length())
		offset := messageSet.Encode(value, 0)
		value = value[:offset]
		compressedValue, err := p.compressor.Compress(value)
		if err != nil {
			return fmt.Errorf("compress messageset error:%s", err)
		}
		var message *Message = &Message{
			Offset:      0,
			MessageSize: 0, // compute in message encode

			Crc:        0, // compute in message encode
			Attributes: 0x00 | p.compressionValue,
			MagicByte:  int8(p.config.HealerMagicByte),
			Key:        nil,
			Value:      compressedValue,
		}
		if p.config.HealerMagicByte == 1 {
			message.Timestamp = uint64(time.Now().UnixMilli())
		}
		messageSet = []*Message{message}
	}
	produceRequest.TopicBlocks[0].PartitonBlocks[0].Partition = p.partition
	produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSetSize = int32(len(messageSet))
	produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSet = messageSet

	rp, err := p.leader.RequestAndGet(produceRequest)
	if err != nil {
		glog.Errorf("produce request error: %s", err)
		return err
	}
	response := rp.(ProduceResponse)
	if glog.V(10) {
		b, _ := json.Marshal(response)
		glog.Infof("produces response: %s", b)
	}
	if err != nil {
		glog.Errorf("decode produce response error: %s", err)
	}
	return err
}

// Close closes the producer
func (p *SimpleProducer) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()

	glog.Info("flush before SimpleProducer close")
	if len(p.messageSet) > 0 {
		messageSet := p.messageSet
		p.messageSet = make([]*Message, 0, p.config.MessageMaxCount)
		p.flush(messageSet)
	}

	glog.Infof("close connection to %s-%d", p.topic, p.partition)
	p.leader.Close()

	close(p.closeChan)
	p.closed = true
}
