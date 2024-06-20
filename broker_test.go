package healer

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

func TestNewBroker(t *testing.T) {
	mockey.PatchConvey("TestListGroupsNil", t, func() {
		mockey.Mock((*Brokers).BrokersInfo).Return(nil).Build()
		groups, err := c.ListGroups()
		convey.So(err, convey.ShouldBeNil)
		convey.So(groups, convey.ShouldBeEmpty)
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
