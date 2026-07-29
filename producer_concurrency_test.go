package healer

import (
	"fmt"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

// buildMetadata returns a MetadataResponse for `topic` with `numPartitions`
// partitions, all healthy (error code 0) and led by broker 0.
func buildMetadata(topic string, numPartitions int) MetadataResponse {
	parts := make([]*PartitionMetadataInfo, numPartitions)
	for i := 0; i < numPartitions; i++ {
		parts[i] = &PartitionMetadataInfo{
			PartitionErrorCode: 0,
			PartitionID:        int32(i),
			Leader:             0,
		}
	}
	return MetadataResponse{
		TopicMetadatas: []TopicMetadata{
			{
				TopicErrorCode:     0,
				TopicName:          topic,
				PartitionMetadatas: parts,
			},
		},
	}
}

// mockProducerBrokers wires up the broker layer so a Producer can be created and
// route messages without touching the network.
func mockProducerBrokers(topic string, numPartitions int) {
	mockey.Mock(NewBrokersWithConfig).Return(&Brokers{}, nil).Build()
	mockey.Mock((*Brokers).RequestMetaData).Return(buildMetadata(topic, numPartitions), nil).Build()
	mockey.Mock((*Brokers).findLeader).Return(int32(0), nil).Build()
	mockey.Mock((*Brokers).NewBroker).Return(&Broker{}, nil).Build()
}

// TestProducerConcurrentMapAccess proves issue #1:
// Producer.getSimpleProducer (key != nil branch) reads and writes
// p.pidToSimpleProducers and p.leaderBrokersMapping WITHOUT holding p.lock.
// Concurrent AddMessage(key, ...) calls therefore race on those maps.
//
// Under -race this reports a DATA RACE on the maps; without -race it can also
// crash the process with "fatal error: concurrent map writes".
//
// Run with:
//
//	go test -gcflags="all=-N -l" -race -run TestProducerConcurrentMapAccess ./
func TestProducerConcurrentMapAccess(t *testing.T) {
	mockey.PatchConvey("concurrent map access in getSimpleProducer", t, func() {
		topic := "testTopic"
		mockProducerBrokers(topic, 16)

		config := map[string]interface{}{
			"bootstrap.servers":   "127.0.0.1:9092",
			"client.id":           "test-producer",
			"message.max.count":   1000000, // huge so AddMessage never triggers a network flush
			"flush.interval.ms":   600000,  // huge so the flush ticker never fires during the test
			"metadata.max.age.ms": 600000,  // huge so the refresh ticker never fires
		}

		producer, err := NewProducer(topic, config)
		convey.So(err, convey.ShouldBeNil)

		const goroutines = 50
		const perGoroutine = 40

		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for j := 0; j < perGoroutine; j++ {
					// distinct keys -> many partitions -> concurrent creation and
					// map writes inside getSimpleProducer.
					key := []byte(fmt.Sprintf("k-%d-%d", g, j))
					_ = producer.AddMessage(key, []byte("v"))
				}
			}(g)
		}
		wg.Wait()
	})
}

// TestProducerTopicMetaRace proves issue #2 (and #3):
// The background refresh path (updateCurrentSimpleProducer) writes p.topicMeta
// WITHOUT holding p.lock, while getSimpleProducer / getLeaderID read
// p.topicMeta.PartitionMetadatas WITHOUT holding p.lock. These race.
//
// Run with:
//
//	go test -gcflags="all=-N -l" -race -run TestProducerTopicMetaRace ./
func TestProducerTopicMetaRace(t *testing.T) {
	mockey.PatchConvey("topicMeta read/write race", t, func() {
		topic := "testTopic"
		mockProducerBrokers(topic, 16)

		config := map[string]interface{}{
			"bootstrap.servers":   "127.0.0.1:9092",
			"client.id":           "test-producer",
			"message.max.count":   1000000,
			"flush.interval.ms":   600000,
			"metadata.max.age.ms": 600000,
		}

		producer, err := NewProducer(topic, config)
		convey.So(err, convey.ShouldBeNil)

		var wg sync.WaitGroup

		// writers: refresh metadata (writes p.topicMeta unlocked)
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					_ = producer.updateCurrentSimpleProducer()
				}
			}()
		}

		// readers: AddMessage with key reads p.topicMeta unlocked
		for r := 0; r < 8; r++ {
			wg.Add(1)
			go func(r int) {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					key := []byte(fmt.Sprintf("r-%d-%d", r, j))
					_ = producer.AddMessage(key, []byte("v"))
				}
			}(r)
		}

		wg.Wait()
	})
}
