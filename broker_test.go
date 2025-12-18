package healer

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

func newMockBroker() *Broker {
	return &Broker{
		address: "localhost:9092",
		nodeID:  0,
		config:  &BrokerConfig{},
		conn:    &mockConn{},
	}
}

func TestNewBroker(t *testing.T) {
	mockey.PatchConvey("TestNewBroker", t, func() {
		mockey.Mock((*Broker).requestAPIVersions).Return(APIVersionsResponse{}, nil).Build()
		mockey.Mock((*net.Dialer).Dial).Return(&mockConn{}, nil).Build()
		broker, err := NewBroker("127.0.0.1:9092", 0, DefaultBrokerConfig())
		convey.So(err, convey.ShouldBeNil)
		convey.So(broker, convey.ShouldNotBeNil)
	})
}
func TestLockInNewBroker(t *testing.T) {
	mockey.PatchConvey("failed Dial could not result in dead lock", t, func() {
		errMock := errors.New("mock dial error")
		dial := mockey.Mock((*net.Dialer).Dial).Return(nil, errMock).Build()
		broker, err := NewBroker("127.0.0.1:9092", 0, DefaultBrokerConfig())
		convey.So(dial.Times(), convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(broker, convey.ShouldBeNil)
	})
}

func TestGetHighestAvailableAPIVersion(t *testing.T) {
	mockey.PatchConvey("TestgetHighestAvailableAPIVersion", t, func() {
		key := API_MetadataRequest
		for _, c := range []struct {
			apiVersion APIVersion
			want       uint16
		}{
			{apiVersion: APIVersion{ApiKey(key), 1, 10}, want: 7},
			{apiVersion: APIVersion{ApiKey(key), 1, 6}, want: 4},
			{apiVersion: APIVersion{ApiKey(key), 2, 10}, want: 7},
			{apiVersion: APIVersion{ApiKey(key), 5, 6}, want: 0},
		} {
			broker := &Broker{
				apiVersions: []APIVersion{
					c.apiVersion,
				},
			}
			got := broker.getHighestAvailableAPIVersion(key)
			convey.So(got, convey.ShouldEqual, c.want)
		}

		broker := &Broker{}
		got := broker.getHighestAvailableAPIVersion(1024)
		convey.So(got, convey.ShouldEqual, 0)
	})
}

func TestReopenConn(t *testing.T) {
	mockey.PatchConvey("conn EOF and reopen new conn", t, func() {
		mockey.Mock(newAPIVersionsResponse).Return(APIVersionsResponse{}, nil).Build()
		mockey.Mock((*net.Dialer).Dial).Return(&mockConn{}, nil).Build()
		mockey.Mock(NewMetadataResponse).Return(MetadataResponse{}, nil).Build()
		brokerCloseOrigin := (*Broker).Close
		brokerClose := mockey.Mock((*Broker).Close).To(func(broker *Broker) { (brokerCloseOrigin)(broker) }).Origin(&brokerCloseOrigin).Build()
		ensureOpenOrigin := (*Broker).ensureOpen
		ensureOpen := mockey.Mock((*Broker).ensureOpen).To(func(broker *Broker) error { return (ensureOpenOrigin)(broker) }).Origin(&ensureOpenOrigin).Build()
		createConnOrigin := (*Broker).createConn
		createConn := mockey.Mock((*Broker).createConn).To(func(broker *Broker) error { return (createConnOrigin)(broker) }).Origin(&createConnOrigin).Build()

		broker, err := NewBroker("127.0.0.1:9092", 0, DefaultBrokerConfig())
		convey.So(err, convey.ShouldBeNil)
		convey.So(broker, convey.ShouldNotBeNil)

		convey.So(ensureOpen.Times(), convey.ShouldEqual, 0)
		convey.So(createConn.Times(), convey.ShouldEqual, 1)

		failCount := 0
		mockey.Mock(defaultReadParser.Read).
			To(func() ([]byte, error) {
				if failCount == 0 {
					failCount++
					return nil, io.EOF
				}
				return make([]byte, 0), nil
			}).Build()

		req := NewMetadataRequest("healer-unittest", []string{"test-topic"})
		_, err = broker.RequestAndGet(req) // EOF
		convey.So(errors.Is(err, io.EOF), convey.ShouldBeTrue)
		convey.So(brokerClose.Times(), convey.ShouldEqual, 1)
		convey.So(ensureOpen.Times(), convey.ShouldEqual, 1)
		convey.So(createConn.Times(), convey.ShouldEqual, 1)

		_, err = broker.RequestAndGet(req)
		convey.So(err, convey.ShouldBeNil)

		convey.So(brokerClose.Times(), convey.ShouldEqual, 1)
		convey.So(ensureOpen.Times(), convey.ShouldEqual, 2)
		convey.So(createConn.Times(), convey.ShouldEqual, 2)
	})
}

func TestRequestLock(t *testing.T) {
	mockey.PatchConvey("ONE broker do Request in multi goroutines in the same time. One closes because of EOF, others should reopen and then complete Request, no nil point panic", t, func() {
		mockey.Mock(newAPIVersionsResponse).Return(APIVersionsResponse{}, nil).Build()
		mockey.Mock((*net.Dialer).Dial).Return(&mockConn{}, nil).Build()
		mockey.Mock(NewMetadataResponse).Return(MetadataResponse{}, nil).Build()

		broker, err := NewBroker("127.0.0.1:9092", 0, DefaultBrokerConfig())
		convey.So(err, convey.ShouldBeNil)
		convey.So(broker, convey.ShouldNotBeNil)

		failCount := 0
		mockey.Mock(defaultReadParser.Read).
			To(func() ([]byte, error) {
				if failCount%2 == 0 {
					failCount++
					return nil, io.EOF
				}
				return make([]byte, 0), nil
			}).Build()

		count := 10
		wg := sync.WaitGroup{}
		wg.Add(count)
		for i := 0; i < count; i++ {
			go func() {
				req := NewMetadataRequest("healer-unittest", []string{"test-topic"})
				_, err = broker.RequestAndGet(req)
				wg.Done()
			}()
		}
		wg.Wait()
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestDeadLockInSasl(t *testing.T) {
	mockey.PatchConvey("deadlock in sasl", t, func() {
		// 模拟 NewBroker 成功返回
		mockey.Mock(newAPIVersionsResponse).Return(APIVersionsResponse{}, nil).Build()
		mockey.Mock((*net.Dialer).Dial).Return(&mockConn{}, nil).Build()
		mockey.Mock(NewMetadataResponse).Return(MetadataResponse{}, nil).Build()

		readCount := 0
		mockey.Mock(defaultReadParser.Read).
			To(func() ([]byte, error) {
				readCount++
				// 1. NewBroker -> SASL Handshake (API 17) -> 成功
				// 2. NewBroker -> SASL Authenticate (API 36) -> 成功
				if readCount <= 2 {
					return make([]byte, 4), nil
				}

				// 3. 第一次显式 RequestAndGet 失败 (模拟网络断开)
				if readCount == 3 {
					return nil, io.EOF
				}

				// 后续重连请求返回 4 字节的长度头
				return make([]byte, 4), nil
			}).Build()

		requestAPIVersionsOrigin := (*Broker).requestAPIVersions
		mockey.Mock((*Broker).requestAPIVersions).To(func(b *Broker, clientID string) (APIVersionsResponse, error) {
			// 直接返回成功，不走网络 IO，避免 ReadAndParse 的干扰
			return APIVersionsResponse{}, nil
		}).Origin(&requestAPIVersionsOrigin).Build()

		// 模拟 SASL 握手响应
		mockey.Mock((*SaslHandshakeResponse).Error).Return(nil).Build()
		mockey.Mock(NewSaslHandshakeResponse).Return(SaslHandshakeResponse{
			CorrelationID: 10,
			ErrorCode:     0,
			EnabledMechanisms: []string{"PLAIN"},
		}, nil).Build()

		// 模拟 SASL 认证响应
		mockey.Mock((*SaslAuthenticateResponse).Error).Return(nil).Build()
		mockey.Mock(NewSaslAuthenticateResponse).Return(SaslAuthenticateResponse{
			CorrelationID: 11,
			ErrorCode:     0,
		}, nil).Build()

		// 配置 SASL
		config := DefaultBrokerConfig()
		config.Sasl = SaslConfig{
			Mechanism: "PLAIN",
			User:      "user",
			Password:  "password",
		}

		broker, err := NewBroker("127.0.0.1:9092", 0, config)
		// 如果这里报错，说明 Mock 有问题，直接失败
		if err != nil {
			t.Fatalf("NewBroker failed: %v", err)
		}
		convey.So(broker, convey.ShouldNotBeNil)

		// 使用 goroutine 和超时机制来检测死锁
		done := make(chan struct{})
		// 由于 goconvey 不能在 goroutine 中使用 So，我们需要把错误传出来
		var goroutineErr error

		go func() {
			defer close(done)
			req := NewMetadataRequest("healer-unittest", []string{"test-topic"})
			// 第一次调用应该触发 EOF 和 Close
			_, err = broker.RequestAndGet(req)
			if !errors.Is(err, io.EOF) {
				goroutineErr = fmt.Errorf("expected EOF, got %v", err)
				return
			}

			// 第二次调用应该触发重连 + SASL认证
			// 如果有死锁，这里会卡住
			// 我们只关心是否卡住。如果没卡住但报错 EOF，说明死锁问题已解决（因为能走到报错这一步）
			_, err = broker.RequestAndGet(req)
			// 这里不需要断言 err，只要不卡住就行
		}()

		select {
		case <-done:
			if goroutineErr != nil {
				t.Fatal(goroutineErr)
			}
			// 测试通过
		case <-time.After(2 * time.Second):
			// 超时则认为发生死锁
			t.Fatal("Deadlock detected during SASL reconnection")
		}
	})
}
