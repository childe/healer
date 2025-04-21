package healer

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ErrProducerClosed is returned when adding message while producer is closed
var ErrProducerClosed = fmt.Errorf("producer closed")

// SimpleProducer is a simple producer that send message to certain one topic-partition
type SimpleProducer struct {
	config *ProducerConfig

	brokers *Brokers
	leader  *Broker
	parent  *Producer

	topic     string
	partition int32

	records []*Record

	lock sync.Mutex

	closeChan chan struct{}
	closed    bool

	compressionValue CompressType
	compressor       Compressor

	once sync.Once
}

func (p *SimpleProducer) createLeader() (*Broker, error) {
	if p.brokers == nil {
		brokerConfig := getBrokerConfigFromProducerConfig(p.config)
		brokers, err := NewBrokersWithConfig(p.config.BootstrapServers, brokerConfig)
		if err != nil {
			logger.Error(err, "failed to init brokers")
			return nil, err
		}
		p.brokers = brokers
	}

	leaderID, err := p.brokers.findLeader(p.config.ClientID, p.topic, p.partition)
	if err != nil || leaderID == -1 {
		logger.Error(err, "could not get leader", "topic", p.topic, "partitionID", p.partition)
		return nil, err
	}
	logger.V(3).Info("leader ID of %s-%d is %d", p.topic, p.partition, leaderID)

	leader, err := p.brokers.NewBroker(leaderID)
	if err == nil {
		logger.V(5).Info("created leader", "topic", p.topic, "partition", p.partition, "leader", leader)
	} else {
		logger.Error(err, "failed to create leader")
		return nil, err
	}

	return leader, err
}

// NewSimpleProducer creates a new simple producer
// config can be a map[string]interface{} or a ProducerConfig,
// use DefaultProducerConfig if config is nil
func NewSimpleProducer(ctx context.Context, topic string, partition int32, config any) (*SimpleProducer, error) {
	cfg, err := createProducerConfig(config)
	logger.Info("create simple producer", "origin_config", config, "final_config", cfg)
	if cfg.HealerMagicByte < 2 {
		cfg.HealerMagicByte = 2
		logger.Info("ONLY SUPPORT MAGIC BYTE 2 FOR NOW, CHANGE IT TO 2")
	}
	if err != nil {
		return nil, err
	}

	p := &SimpleProducer{
		config:    &cfg,
		topic:     topic,
		partition: partition,

		closeChan: make(chan struct{}),
	}

	switch cfg.CompressionType {
	case "none":
		p.compressionValue = CompressionNone
	case "gzip":
		p.compressionValue = CompressionGzip
	case "snappy":
		p.compressionValue = CompressionSnappy
	case "lz4":
		p.compressionValue = CompressionLz4
	default:
		return nil, fmt.Errorf("unknown compress type")
	}
	p.compressor = NewCompressor(cfg.CompressionType)

	p.records = make([]*Record, 0, cfg.MessageMaxCount)

	leader := ctx.Value(leaderKey)
	if leader != nil {
		p.leader = leader.(*Broker)
	} else {
		p.leader, err = p.createLeader()
		if err != nil {
			return nil, fmt.Errorf("create producer leader error: %w", err)
		}
	}

	parent := ctx.Value(parentProducerKey)
	if parent != nil {
		p.parent = parent.(*Producer)
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

	return p, nil
}

// AddMessage add message to message set. If message set is full, send it to kafka synchronously
func (p *SimpleProducer) AddMessage(key []byte, value []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.closed {
		return ErrProducerClosed
	}

	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	record := Record{
		attributes:     0x00, // compress in upper message set level
		timestampDelta: 0,
		offsetDelta:    0,

		key:   key,
		value: valueCopy,
	}

	p.records = append(p.records, &record)
	if len(p.records) >= p.config.MessageMaxCount {
		records := p.records
		p.records = make([]*Record, 0, p.config.MessageMaxCount)
		p.flush(records)
	}
	return nil
}

// Flush send all messages to kafka
func (p *SimpleProducer) Flush() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.records) > 0 {
		records := p.records
		p.records = make([]*Record, 0, p.config.MessageMaxCount)
		return p.flush(records)
	}
	return nil
}

func (p *SimpleProducer) createRecordBatch(records []*Record) RecordBatch {
	batch := RecordBatch{
		BaseOffset:           0,
		BatchLength:          0,
		PartitionLeaderEpoch: -1,
		Magic:                int8(p.config.HealerMagicByte),
		CRC:                  0,
		Attributes:           int16(0 | p.compressionValue),
		LastOffsetDelta:      int32(len(records)) - 1,
		BaseTimestamp:        0,
		MaxTimestamp:         0,
		ProducerID:           -1,
		ProducerEpoch:        0,
		BaseSequence:         0,
		Records:              records,
	}

	return batch
}

func (p *SimpleProducer) flush(records []*Record) error {
	logger.V(5).Info("flush messsages", "count", len(records), "topic", p.topic, "partition", p.partition)

	produceRequest := &ProduceRequest{
		RequiredAcks: p.config.Acks,
		Timeout:      p.config.RequestTimeoutMS,
	}
	produceRequest.RequestHeader = &RequestHeader{
		APIKey:     API_ProduceRequest,
		APIVersion: 0,
		ClientID:   &p.config.ClientID,
	}

	produceRequest.TopicBlocks = make([]produceRequestT, 1)
	produceRequest.TopicBlocks[0].TopicName = p.topic
	produceRequest.TopicBlocks[0].PartitonBlocks = make([]produceRequestP, 1)

	produceRequest.TopicBlocks[0].PartitonBlocks[0].Partition = p.partition
	produceRequest.TopicBlocks[0].PartitonBlocks[0].RecordBatches = []RecordBatch{
		p.createRecordBatch(records),
	}

	rp, err := p.leader.RequestAndGet(produceRequest)
	if err == nil {
		err = rp.Error()
	}
	if err != nil {
		logger.Error(err, "failed to produce request")
		return err
	}
	return err
}

// Close closes the producer
func (p *SimpleProducer) Close() {
	p.once.Do(func() {
		p.lock.Lock()
		defer p.lock.Unlock()

		logger.Info("flush before SimpleProducer close")
		if len(p.records) > 0 {
			p.flush(p.records)
		}

		if p.parent == nil {
			logger.Info("close connection to leader")
			p.leader.Close()
		} else {
			logger.Info("connection not closed here, parent will close it")
		}

		close(p.closeChan)
		p.closed = true
	})
}
