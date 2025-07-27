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

func TestDeleteGroups(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestDeleteGroups", t, func() {
		mockey.Mock((*Brokers).FindCoordinator).Return(FindCoordinatorResponse{
			Coordinator: Coordinator{
				NodeID: 1,
				Host:   "localhost",
				Port:   9092,
			},
		}, nil).Build()
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{}, nil).Build()
		mockey.Mock((*Broker).RequestAndGet).
			To(func(b *Broker, req Request) (Response, error) {
				mockResponse := DeleteGroupsResponse{
					CorrelationID:  1,
					ThrottleTimeMs: 0,
					Results: []struct {
						GroupID   string `json:"group_id"`
						ErrorCode int16  `json:"error_code"`
					}{
						{
							GroupID:   "test-group",
							ErrorCode: 0, // No error
						},
					},
				}
				return mockResponse, nil
			}).Build()

		// Test successful group deletion
		groups := []string{"test-group"}
		response, err := c.DeleteGroups(groups)
		convey.So(err, convey.ShouldBeNil)
		convey.So(response.Results, convey.ShouldHaveLength, 1)
		convey.So(response.Results[0].GroupID, convey.ShouldEqual, "test-group")
		convey.So(response.Results[0].ErrorCode, convey.ShouldEqual, 0)
	})
}

func TestDeleteMultipleGroups(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestDeleteMultipleGroups", t, func() {
		mockey.Mock((*Brokers).FindCoordinator).Return(FindCoordinatorResponse{
			Coordinator: Coordinator{
				NodeID: 1,
				Host:   "localhost",
				Port:   9092,
			},
		}, nil).Build()
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{}, nil).Build()
		callCount := 0
		mockey.Mock((*Broker).RequestAndGet).
			To(func(b *Broker, req Request) (Response, error) {
				callCount++
				groupName := fmt.Sprintf("test-group-%d", callCount)
				mockResponse := DeleteGroupsResponse{
					CorrelationID:  uint32(callCount),
					ThrottleTimeMs: 0,
					Results: []struct {
						GroupID   string `json:"group_id"`
						ErrorCode int16  `json:"error_code"`
					}{
						{
							GroupID:   groupName,
							ErrorCode: 0,
						},
					},
				}
				return mockResponse, nil
			}).Build()

		// Test successful deletion of multiple groups
		groups := []string{"test-group-1", "test-group-2"}
		response, err := c.DeleteGroups(groups)
		convey.So(err, convey.ShouldBeNil)
		convey.So(response.Results, convey.ShouldHaveLength, 2)
		convey.So(response.Results[0].ErrorCode, convey.ShouldEqual, 0)
		convey.So(response.Results[1].ErrorCode, convey.ShouldEqual, 0)
	})
}

func TestDeleteGroupsWithError(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestDeleteGroupsWithError", t, func() {
		mockey.Mock((*Brokers).FindCoordinator).Return(FindCoordinatorResponse{
			Coordinator: Coordinator{
				NodeID: 1,
				Host:   "localhost",
				Port:   9092,
			},
		}, nil).Build()
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{}, nil).Build()
		mockey.Mock((*Broker).RequestAndGet).
			To(func(b *Broker, req Request) (Response, error) {
				mockResponse := DeleteGroupsResponse{
					CorrelationID:  1,
					ThrottleTimeMs: 0,
					Results: []struct {
						GroupID   string `json:"group_id"`
						ErrorCode int16  `json:"error_code"`
					}{
						{
							GroupID:   "nonexistent-group",
							ErrorCode: 69, // GroupIdNotFound error
						},
					},
				}
				return mockResponse, nil
			}).Build()

		// Test group deletion with error (group not found)
		groups := []string{"nonexistent-group"}
		response, err := c.DeleteGroups(groups)
		convey.So(err, convey.ShouldBeNil)
		convey.So(response.Results, convey.ShouldHaveLength, 1)
		convey.So(response.Results[0].GroupID, convey.ShouldEqual, "nonexistent-group")
		convey.So(response.Results[0].ErrorCode, convey.ShouldEqual, 69)

		// Test that the response has an error
		responseError := response.Error()
		convey.So(responseError, convey.ShouldNotBeNil)
	})
}
