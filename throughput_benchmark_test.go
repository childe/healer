package healer

import (
	"bytes"
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"k8s.io/klog/v2"
)

// A representative message payload (an nginx access-log line) used across the
// throughput benchmarks so produce and consume numbers are comparable.
var benchValue = []byte(`192.168.1.100 - - [16/Apr/2024:00:01:22 +0800] "GET /index.html HTTP/1.1" 200 1534 "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"`)

var benchKey = []byte("key-1")

// silenceLogger disables logging so it does not skew benchmark timings.
func silenceLogger() {
	SetLogger(klog.NewKlogr().WithName("healer-bench").WithSink(nil))
}

// BenchmarkSimpleProducerAddMessage measures the client-side produce path:
// AddMessage -> append to batch -> (every message.max.count) createRecordBatch
// + flush. The network round trip is mocked out, so this isolates the CPU and
// allocation cost the client itself pays per produced message.
//
//	go test -gcflags="all=-N -l" -run '^$' -bench BenchmarkSimpleProducerAddMessage -benchmem ./
func BenchmarkSimpleProducerAddMessage(b *testing.B) {
	silenceLogger()

	// No-op network: measure batching + serialization-into-batch only.
	// Plain Mock (not PatchConvey) so convey does not write to stdout and mangle
	// the benchmark result line under -count/-benchtime.
	mockey.Mock((*Broker).RequestAndGet).Return(ProduceResponse{}, nil).Build()
	defer mockey.UnPatchAll()

	config := map[string]interface{}{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "healer-bench",
		"message.max.count": 500,     // flush every 500 records
		"flush.interval.ms": 3600000, // disable the time-based flush during the run
		"compress.type":     "none",
	}

	// Carry a fake leader in ctx so NewSimpleProducer never touches the network.
	ctx := context.WithValue(context.Background(), leaderKey, &Broker{})
	p, err := NewSimpleProducer(ctx, "testTopic", 0, config)
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()

	b.ReportAllocs()
	b.SetBytes(int64(len(benchValue)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.AddMessage(benchKey, benchValue); err != nil {
			b.Fatal(err)
		}
	}
}

// buildProduceRequest builds a ProduceRequest carrying a single record batch of
// numRecords messages, ready to Encode.
func buildProduceRequest(numRecords int) *ProduceRequest {
	clientID := "healer-bench"
	records := make([]*Record, numRecords)
	for i := 0; i < numRecords; i++ {
		records[i] = &Record{
			attributes:     0,
			timestampDelta: int64(i),
			offsetDelta:    int32(i),
			key:            benchKey,
			value:          benchValue,
			Headers:        []RecordHeader{},
		}
	}
	return &ProduceRequest{
		RequestHeader: &RequestHeader{
			APIKey:     API_ProduceRequest,
			APIVersion: 7,
			ClientID:   &clientID,
		},
		RequiredAcks: 1,
		Timeout:      5000,
		TopicBlocks: []produceRequestT{
			{
				TopicName: "test-topic",
				PartitonBlocks: []produceRequestP{
					{
						Partition: 0,
						RecordBatches: []RecordBatch{
							{
								Magic:           2,
								LastOffsetDelta: int32(numRecords) - 1,
								BaseTimestamp:   1630000000000,
								MaxTimestamp:    1630000010000,
								ProducerID:      -1,
								Records:         records,
							},
						},
					},
				},
			},
		},
	}
}

// BenchmarkProduceRequestEncode measures pure produce serialization: turning a
// batch of records into the on-the-wire byte payload (the cost skipped by the
// mocked network in BenchmarkSimpleProducerAddMessage).
//
//	go test -gcflags="all=-N -l" -run '^$' -bench BenchmarkProduceRequestEncode -benchmem ./
func BenchmarkProduceRequestEncode(b *testing.B) {
	const numRecords = 500
	var version uint16 = 7
	request := buildProduceRequest(numRecords)

	payloadLen := len(request.Encode(version))

	b.ReportAllocs()
	b.SetBytes(int64(payloadLen))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = request.Encode(version)
	}
}

// buildFetchPayload builds the on-the-wire body of a FetchResponse carrying a
// single record batch of numRecords messages, as the decoder would read it.
func buildFetchPayload(topic string, numRecords int, version uint16) []byte {
	records := make([]*Record, numRecords)
	for i := 0; i < numRecords; i++ {
		records[i] = &Record{
			attributes:     0,
			timestampDelta: int64(i),
			offsetDelta:    int32(i),
			key:            benchKey,
			value:          benchValue,
			Headers:        []RecordHeader{},
		}
	}
	fr := FetchResponse{
		CorrelationID: 1,
		Responses: map[string][]PartitionResponse{
			topic: {
				{
					PartitionID:      0,
					ErrorCode:        0,
					HighWatermark:    int64(numRecords),
					LastStableOffset: int64(numRecords),
					RecordBatches: []RecordBatch{
						{
							BaseOffset:      0,
							Magic:           2,
							LastOffsetDelta: int32(numRecords) - 1,
							BaseTimestamp:   1630000000000,
							MaxTimestamp:    1630000010000,
							ProducerID:      -1,
							Records:         records,
						},
					},
				},
			},
		},
	}
	payload, _ := fr.Encode(version)
	return payload
}

// BenchmarkFetchResponseDecode measures pure consume deserialization: streaming
// a fetch response payload through the decoder and emitting FullMessages. This
// is the consumer's CPU/allocation hot path, isolated from the network.
//
//	go test -gcflags="all=-N -l" -run '^$' -bench BenchmarkFetchResponseDecode -benchmem ./
func BenchmarkFetchResponseDecode(b *testing.B) {
	silenceLogger()

	const numRecords = 500
	var version uint16 = 10
	payload := buildFetchPayload("test-topic", numRecords, version)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messages := make(chan *FullMessage, numRecords+16)
		decoder := &fetchResponseStreamDecoder{
			ctx:         context.Background(),
			buffers:     bytes.NewReader(payload),
			messages:    messages,
			totalLength: len(payload),
			version:     version,
		}

		done := make(chan struct{})
		go func() {
			for range messages {
			}
			close(done)
		}()

		decoder.streamDecode(context.Background(), 0)
		close(messages)
		<-done
	}
}
