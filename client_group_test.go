package healer

import (
	"errors"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

func TestListGroups(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestListGroupsNil", t, func() {
		mockey.Mock((*Brokers).BrokersInfo).Return(nil).Build()
		groups, err := c.ListGroups()
		convey.So(err, convey.ShouldBeNil)
		convey.So(groups, convey.ShouldBeEmpty)
	})
	mockey.PatchConvey("TestListGroupsEmpty", t, func() {
		mockey.Mock((*Brokers).BrokersInfo).Return(map[int32]*BrokerInfo{}).Build()
		groups, err := c.ListGroups()
		convey.So(err, convey.ShouldBeNil)
		convey.So(groups, convey.ShouldBeEmpty)
	})
	mockey.PatchConvey("TestListGroups", t, func() {
		errMock := errors.New("mock error")
		broker := BrokerInfo{}
		mockey.Mock((*Brokers).BrokersInfo).Return(map[int32]*BrokerInfo{
			1: &broker,
		}).Build()
		mockey.Mock((*Brokers).GetBroker).Return(nil, errMock).Build()
		groups, err := c.ListGroups()
		convey.So(err, convey.ShouldEqual, errMock)
		convey.So(groups, convey.ShouldBeEmpty)
	})
	mockey.PatchConvey("TestListGroups", t, func() {
		errMock := errors.New("mock error")
		mockey.Mock((*Brokers).BrokersInfo).Return(map[int32]*BrokerInfo{
			1: {},
		}).Build()
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{}, nil).Build()
		mockey.Mock((*Broker).RequestListGroups).Return(nil, errMock).Build()
		groups, err := c.ListGroups()
		convey.So(err, convey.ShouldEqual, errMock)
		convey.So(groups, convey.ShouldBeEmpty)
	})
	mockey.PatchConvey("TestListGroups", t, func() {
		brokerID := int32(1)
		mockey.Mock((*Brokers).BrokersInfo).Return(map[int32]*BrokerInfo{
			brokerID: {NodeID: brokerID},
		}).Build()
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{nodeID: brokerID}, nil).Build()
		mockey.Mock((*Broker).RequestListGroups).Return(&ListGroupsResponse{
			Groups: []*Group{
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
		convey.So(len(groups), convey.ShouldEqual, 1)
		fmt.Println(groups)
		convey.So(len(groups[1]), convey.ShouldEqual, 2)
	})
}
