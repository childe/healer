package client

import (
	"errors"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/childe/healer"
	"github.com/smartystreets/goconvey/convey"
)

func TestListGroups(t *testing.T) {
	brokers := &healer.Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestListGroups", t, func() {
		mockey.Mock(brokers.BrokersInfo).Return(nil).Build()
		groups, err := c.ListGroups()
		convey.So(err, convey.ShouldBeNil)
		convey.So(groups, convey.ShouldBeEmpty)
	})
	mockey.PatchConvey("TestListGroups", t, func() {
		mockey.Mock(brokers.BrokersInfo).Return(map[int32]*healer.BrokerInfo{}).Build()
		groups, err := c.ListGroups()
		convey.So(err, convey.ShouldBeNil)
		convey.So(groups, convey.ShouldBeEmpty)
	})
	mockey.PatchConvey("TestListGroups", t, func() {
		errMock := errors.New("mock error")
		broker := healer.BrokerInfo{}
		mockey.Mock((*healer.Brokers).BrokersInfo).Return(map[int32]*healer.BrokerInfo{
			1: &broker,
		}).Build()
		mockey.Mock((*healer.Brokers).GetBroker).Return(nil, errMock).Build()
		groups, err := c.ListGroups()
		convey.So(err, convey.ShouldEqual, errMock)
		convey.So(groups, convey.ShouldBeEmpty)
	})
	mockey.PatchConvey("TestListGroups", t, func() {
		errMock := errors.New("mock error")
		broker := healer.BrokerInfo{}
		mockey.Mock((*healer.Brokers).BrokersInfo).Return(map[int32]*healer.BrokerInfo{
			1: &broker,
		}).Build()
		mockey.Mock((*healer.Brokers).GetBroker).Return(&healer.Broker{}, nil).Build()
		mockey.Mock((*healer.Broker).RequestListGroups).Return(nil, errMock).Build()
		groups, err := c.ListGroups()
		convey.So(err, convey.ShouldEqual, errMock)
		convey.So(groups, convey.ShouldBeEmpty)
	})
	mockey.PatchConvey("TestListGroups", t, func() {
		broker := healer.BrokerInfo{}
		mockey.Mock((*healer.Brokers).BrokersInfo).Return(map[int32]*healer.BrokerInfo{
			1: &broker,
		}).Build()
		mockey.Mock((*healer.Brokers).GetBroker).Return(&healer.Broker{}, nil).Build()
		mockey.Mock((*healer.Broker).RequestListGroups).Return(healer.ListGroupsResponse{
			Groups: []*healer.Group{
				{
					GroupID: "test1",
				},
				{
					GroupID: "test2",
				},
			},
		}, nil).Build()
		groups, err := c.ListGroups()
		convey.So(err, convey.ShouldBeNil)
		convey.So(len(groups), convey.ShouldEqual, 2)
	})
}
