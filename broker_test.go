package healer

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

func newMockBroker() *Broker {
	return &Broker{
		address: "localhost:9092",
		nodeID:  0,
		config:  &BrokerConfig{},
		conn:    &MockConn{},
	}
}

func TestNewBroker(t *testing.T) {
	mockey.PatchConvey("TestNewBroker", t, func() {
		mockey.Mock((*Broker).requestAPIVersions).Return(APIVersionsResponse{}, nil).Build()
		mockey.Mock((*net.Dialer).Dial).Return(&MockConn{}, nil).Build()
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
		mockey.Mock((*net.Dialer).Dial).Return(&MockConn{}, nil).Build()
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
		mockey.Mock((*net.Dialer).Dial).Return(&MockConn{}, nil).Build()
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
