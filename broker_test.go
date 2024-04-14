package healer

import (
	"errors"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

func TestNewBroker(t *testing.T) {
	config := DefaultBrokerConfig()
	mockey.PatchConvey("TestNewBroker", t, func() {
		nodeID := int32(0)
		addr := "127.0.0.1:9092"
		errMock := errors.New("NewBroker")

		mockey.Mock(newConn).Return(nil, errMock).Build()
		_, err := NewBroker(addr, nodeID, config)
		errWant := fmt.Errorf("failed to establish connection when init broker [%d]%s: %v", nodeID, addr, errMock)
		convey.So(err.Error(), convey.ShouldEqual, errWant.Error())
	})

	mockey.PatchConvey("TestNewBroker", t, func() {
		nodeID := int32(0)
		addr := "127.0.0.1:9092"
		errMock := errors.New("NewBroker")

		mockey.Mock(newConn).Return(nil, nil).Build()

		mockey.Mock((*Broker).requestAPIVersions).Return(nil, errMock).Build()
		_, err := NewBroker(addr, nodeID, config)
		errWant := fmt.Errorf("failed to request api versions when init broker: %w", errMock)
		convey.So(err.Error(), convey.ShouldEqual, errWant.Error())
	})

	mockey.PatchConvey("TestNewBroker", t, func() {
		nodeID := int32(0)
		addr := "127.0.0.1:9092"

		mockey.Mock(newConn).Return(nil, nil).Build()
		mockey.Mock((*Broker).requestAPIVersions).Return(nil, nil).Build()

		_, err := NewBroker(addr, nodeID, config)
		convey.So(err, convey.ShouldEqual, nil)
	})
}
