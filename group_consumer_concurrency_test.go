package healer

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

// TestHeartbeatDoesNotBlockStop proves issue #1:
// heartbeat() must NOT hold c.mutex while doing the coordinator network call,
// otherwise a hung/slow heartbeat blocks stop()/Close() (which also needs c.mutex)
// for the whole duration of the network I/O (and writes have no deadline).
//
// Before the fix this test FAILS (stop() blocks for the full timeout).
// After the fix it PASSES (stop() returns promptly even mid-heartbeat).
func TestHeartbeatDoesNotBlockStop(t *testing.T) {
	mockey.PatchConvey("heartbeat must not hold mutex during network IO", t, func() {
		topic := "testTopic"
		config := map[string]interface{}{
			"bootstrap.servers":  "127.0.0.1:9092",
			"group.id":           "healer-test",
			"client.id":          "test-client",
			"session.timeout.ms": 30000,
		}

		mockey.Mock(NewBrokersWithConfig).Return(&Brokers{}, nil).Build()
		mockey.Mock((*Brokers).Close).Return().Build()

		consumer, err := NewGroupConsumer(topic, config)
		convey.So(err, convey.ShouldBeNil)

		consumer.coordinator = &Broker{address: "127.0.0.1:9092"}
		consumer.joined = true

		release := make(chan struct{})
		var releaseOnce sync.Once
		doRelease := func() { releaseOnce.Do(func() { close(release) }) }
		defer doRelease()

		hbStarted := make(chan struct{})

		// Simulate a slow/hung coordinator network call.
		mockey.Mock((*Broker).requestHeartbeat).To(
			func(b *Broker, clientID, groupID string, generationID int32, memberID string) (HeartbeatResponse, error) {
				close(hbStarted)
				<-release // block as if the TCP write/read is stuck
				return HeartbeatResponse{}, nil
			}).Build()

		// Fire a heartbeat; it will (before fix) grab c.mutex and block in the network call.
		go consumer.heartbeat()
		<-hbStarted

		// Now try to stop. stop() needs c.mutex.
		stopped := make(chan struct{})
		go func() {
			consumer.stop()
			close(stopped)
		}()

		select {
		case <-stopped:
			// good: stop proceeded even though a heartbeat network call is in flight
		case <-time.After(2 * time.Second):
			doRelease()
			t.Fatal("stop() blocked while a heartbeat network call was in flight: " +
				"c.mutex is held across coordinator I/O (issue #1)")
		}
		doRelease()
	})
}

// drainMessages keeps reading from the message channel so producers never block on it.
func drainMessages(messages <-chan *FullMessage, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case _, ok := <-messages:
			if !ok {
				return
			}
		}
	}
}

// setupMockedGroupConsumer wires up the same mocks used by TestStopGroupConsumer so a
// GroupConsumer can fully run its goroutines (heartbeat, metadata refresh, consume) without a broker.
func setupMockedGroupConsumer(t *testing.T, topic string, config map[string]interface{}) {
	mockey.Mock((*SimpleConsumer).refreshPartiton).Return(nil).Build()
	mockey.Mock((*SimpleConsumer).getLeaderBroker).Return(nil).Build()
	mockey.Mock((*SimpleConsumer).initOffset).Return().Build()
	mockey.Mock((*SimpleConsumer).getCommitedOffet).Return(nil).Build()
	mockey.Mock((*SimpleConsumer).getOffset).Return(0, nil).Build()
	mockey.Mock((*SimpleConsumer).getCoordinator).Return(nil).Build()
	mockey.Mock((*Broker).getHighestAvailableAPIVersion).Return(0).Build()
	mockey.Mock((*Broker).requestLeaveGroup).Return(LeaveGroupResponse{}, nil).Build()
	mockey.Mock((*Broker).requestHeartbeat).Return(HeartbeatResponse{}, nil).Build()
	mockey.Mock(NewBrokersWithConfig).Return(&Brokers{}, nil).Build()
	mockey.Mock((*Brokers).RequestMetaData).Return(MetadataResponse{}, nil).Build()
	mockey.Mock((*Brokers).Close).Return().Build()
	mockey.Mock((*GroupConsumer).joinAndSync).To(func(c *GroupConsumer) error {
		simpleConsumer, _ := NewSimpleConsumer(topic, 0, config)
		simpleConsumer.belongTO = c
		simpleConsumer.wg = &c.wg
		c.simpleConsumers = append(c.simpleConsumers, simpleConsumer)
		return nil
	}).Build()
	mockey.Mock((*Broker).requestFetchStreamingly).
		To(func(b *Broker, fetchRequest *FetchRequest) (r io.Reader, responseLength uint32, err error) {
			return nil, 0, nil
		}).Build()
	mockey.Mock((*fetchResponseStreamDecoder).streamDecode).To(
		func(decoder *fetchResponseStreamDecoder, ctx context.Context, startOffset int64) error {
			return nil
		}).Build()
}

// TestCloseRacesWithRestart proves issues #2/#3/#5:
//   - AwaitClose()/stop() do not take restartLocker, so they run concurrently with restart()
//   - restart() -> consumeWithoutHeartBeat calls c.wg.Add(1) while AwaitClose's goroutine is in c.wg.Wait()
//     (WaitGroup Add concurrent with Wait -> panic "WaitGroup is reused ..." / "negative counter")
//   - c.closed is a plain bool read/written from several goroutines (data race, caught by -race)
//
// Run with -race to see the data race. The WaitGroup panic (if it fires) is captured and reported.
// Before the fix this test FAILS/races. After the fix it PASSES cleanly.
func TestCloseRacesWithRestart(t *testing.T) {
	mockey.PatchConvey("Close must not race with restart", t, func() {
		topic := "testTopic"
		config := map[string]interface{}{
			"bootstrap.servers":  "127.0.0.1:9092",
			"group.id":           "healer-test",
			"client.id":          "test-client",
			"session.timeout.ms": 6000, // heartbeat ticker fires every 600ms
		}
		setupMockedGroupConsumer(t, topic, config)

		consumer, err := NewGroupConsumer(topic, config)
		convey.So(err, convey.ShouldBeNil)

		messages, err := consumer.Consume(nil)
		convey.So(err, convey.ShouldBeNil)

		stopDrain := make(chan struct{})
		go drainMessages(messages, stopDrain)

		panicCh := make(chan interface{}, 8)
		var wg sync.WaitGroup

		// Hammer restart() concurrently with Close().
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if r := recover(); r != nil {
						panicCh <- r
					}
				}()
				for j := 0; j < 5; j++ {
					consumer.restart()
				}
			}()
		}

		// Close in the middle of the restart storm.
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicCh <- r
				}
			}()
			time.Sleep(20 * time.Millisecond)
			consumer.Close()
		}()

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(15 * time.Second):
			close(stopDrain)
			t.Fatal("Close()/restart() deadlocked or hung (issues #2/#3)")
		}
		close(stopDrain)

		select {
		case r := <-panicCh:
			t.Fatalf("Close() raced with restart() and panicked (issues #2/#3): %v", r)
		default:
		}
	})
}

