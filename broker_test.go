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
		errMock := errors.New("NewBroker")
		mockApiversion := make([]APIVersion, 2)
		mockApiversion[0] = APIVersion{apiKey: 0, minVersion: 1, maxVersion: 10}
		mockApiversion[1] = APIVersion{apiKey: 1, minVersion: 1, maxVersion: 10}
		mockApiversionResponse := APIVersionsResponse{
			ErrorCode:   0,
			APIVersions: mockApiversion,
		}

		mockey.Mock(newConn).Return(nil, nil).Build()
		mockey.Mock((*Broker).requestAPIVersions).Return(mockApiversionResponse, nil).Build()
		mockey.Mock((*Broker).sendSaslAuthenticate).Return(errMock).Build()

		config.SaslConfig = &SaslConfig{}
		_, err := NewBroker(addr, nodeID, config)
		convey.So(err, convey.ShouldEqual, errMock)
	})

	mockey.PatchConvey("TestNewBroker", t, func() {
		nodeID := int32(0)
		addr := "127.0.0.1:9092"

		mockey.Mock(newConn).Return(nil, nil).Build()
		mockey.Mock((*Broker).requestAPIVersions).Return(nil, nil).Build()
		mockey.Mock((*Broker).sendSaslAuthenticate).Return(nil).Build()

		config.SaslConfig = &SaslConfig{}
		_, err := NewBroker(addr, nodeID, config)
		convey.So(err, convey.ShouldEqual, nil)
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
			{apiVersion: APIVersion{ApiKey(key), 1, 6}, want: 1},
			{apiVersion: APIVersion{ApiKey(key), 2, 10}, want: 7},
			{apiVersion: APIVersion{ApiKey(key), 2, 6}, want: 0},
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
