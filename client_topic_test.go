package healer

import (
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

func TestDeleteTopics(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestDeleteTopics", t, func() {
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{}, nil).Build()
		mockey.Mock((*Broker).RequestAndGet).
			To(func(b *Broker, req Request) (Response, error) {
				var mockResponse DeleteTopicsResponse
				return mockResponse, nil
			}).Build()

		topics := []string{"test1", "test2"}
		_, err := c.DeleteTopics(topics, 30000)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestCreateTopic(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestCreateTopic", t, func() {
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{}, nil).Build()
		mockey.Mock((*Broker).RequestAndGet).
			To(func(b *Broker, req Request) (Response, error) {
				mockResponse := CreateTopicsResponse{
					CorrelationID: 1,
					TopicErrors: []TopicError{
						{
							Topic:     "test-topic",
							ErrorCode: 0, // No error
						},
					},
				}
				return mockResponse, nil
			}).Build()

		// Test successful topic creation
		response, err := c.CreateTopic("test-topic", 3, 1, 30000)
		convey.So(err, convey.ShouldBeNil)
		convey.So(response.TopicErrors, convey.ShouldHaveLength, 1)
		convey.So(response.TopicErrors[0].Topic, convey.ShouldEqual, "test-topic")
		convey.So(response.TopicErrors[0].ErrorCode, convey.ShouldEqual, 0)
	})
}

func TestCreateTopics(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestCreateTopics", t, func() {
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{}, nil).Build()
		mockey.Mock((*Broker).RequestAndGet).
			To(func(b *Broker, req Request) (Response, error) {
				mockResponse := CreateTopicsResponse{
					CorrelationID: 1,
					TopicErrors: []TopicError{
						{
							Topic:     "test-topic-1",
							ErrorCode: 0,
						},
						{
							Topic:     "test-topic-2",
							ErrorCode: 0,
						},
					},
				}
				return mockResponse, nil
			}).Build()

		// Test successful multiple topics creation
		topics := []string{"test-topic-1", "test-topic-2"}
		response, err := c.CreateTopics(topics, 3, 1, 30000)
		convey.So(err, convey.ShouldBeNil)
		convey.So(response.TopicErrors, convey.ShouldHaveLength, 2)
		convey.So(response.TopicErrors[0].Topic, convey.ShouldEqual, "test-topic-1")
		convey.So(response.TopicErrors[0].ErrorCode, convey.ShouldEqual, 0)
		convey.So(response.TopicErrors[1].Topic, convey.ShouldEqual, "test-topic-2")
		convey.So(response.TopicErrors[1].ErrorCode, convey.ShouldEqual, 0)
	})
}

func TestCreateTopicWithError(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestCreateTopicWithError", t, func() {
		mockey.Mock((*Brokers).Controller).Return(int32(1)).Build()
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{}, nil).Build()
		mockey.Mock((*Broker).RequestAndGet).
			To(func(b *Broker, req Request) (Response, error) {
				mockResponse := CreateTopicsResponse{
					CorrelationID: 1,
					TopicErrors: []TopicError{
						{
							Topic:     "existing-topic",
							ErrorCode: 36, // TopicAlreadyExists error
						},
					},
				}
				return mockResponse, nil
			}).Build()

		// Test topic creation with error (topic already exists)
		response, err := c.CreateTopic("existing-topic", 3, 1, 30000)
		convey.So(err, convey.ShouldBeNil)
		convey.So(response.TopicErrors, convey.ShouldHaveLength, 1)
		convey.So(response.TopicErrors[0].Topic, convey.ShouldEqual, "existing-topic")
		convey.So(response.TopicErrors[0].ErrorCode, convey.ShouldEqual, 36)

		// Test that the response has an error
		responseError := response.Error()
		convey.So(responseError, convey.ShouldNotBeNil)
	})
}

func TestCreatePartitions(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestCreatePartitions", t, func() {
		mockey.Mock((*Brokers).Controller).Return(int32(1)).Build()
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{}, nil).Build()
		mockey.Mock((*Broker).RequestAndGet).
			To(func(b *Broker, req Request) (Response, error) {
				mockResponse := CreatePartitionsResponse{
					CorrelationID:  1,
					ThrottleTimeMS: 0,
					Results: []createPartitionsResponseResultBlock{
						{
							TopicName:    "test-topic",
							ErrorCode:    0, // No error
							ErrorMessage: nil,
						},
					},
				}
				return mockResponse, nil
			}).Build()

		// Test successful partition creation
		response, err := c.CreatePartitions("test-topic", 6, 30000, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(response.Results, convey.ShouldHaveLength, 1)
		convey.So(response.Results[0].TopicName, convey.ShouldEqual, "test-topic")
		convey.So(response.Results[0].ErrorCode, convey.ShouldEqual, 0)
	})
}

func TestCreatePartitionsWithAssignments(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestCreatePartitionsWithAssignments", t, func() {
		mockey.Mock((*Brokers).Controller).Return(int32(1)).Build()
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{}, nil).Build()
		mockey.Mock((*Broker).RequestAndGet).
			To(func(b *Broker, req Request) (Response, error) {
				mockResponse := CreatePartitionsResponse{
					CorrelationID:  1,
					ThrottleTimeMS: 0,
					Results: []createPartitionsResponseResultBlock{
						{
							TopicName:    "test-topic-assignments",
							ErrorCode:    0,
							ErrorMessage: nil,
						},
					},
				}
				return mockResponse, nil
			}).Build()

		// Test successful partition creation with custom assignments
		assignments := [][]int32{{1, 2}, {2, 3}}
		response, err := c.CreatePartitionsWithAssignments("test-topic-assignments", 5, assignments, 30000, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(response.Results, convey.ShouldHaveLength, 1)
		convey.So(response.Results[0].TopicName, convey.ShouldEqual, "test-topic-assignments")
		convey.So(response.Results[0].ErrorCode, convey.ShouldEqual, 0)
	})
}

func TestCreatePartitionsWithError(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestCreatePartitionsWithError", t, func() {
		mockey.Mock((*Brokers).Controller).Return(int32(1)).Build()
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{}, nil).Build()
		mockey.Mock((*Broker).RequestAndGet).
			To(func(b *Broker, req Request) (Response, error) {
				errorMsg := "Invalid partition count"
				mockResponse := CreatePartitionsResponse{
					CorrelationID:  1,
					ThrottleTimeMS: 0,
					Results: []createPartitionsResponseResultBlock{
						{
							TopicName:    "invalid-topic",
							ErrorCode:    37, // InvalidPartitions error
							ErrorMessage: &errorMsg,
						},
					},
				}
				return mockResponse, nil
			}).Build()

		// Test partition creation with error (invalid partition count)
		response, err := c.CreatePartitions("invalid-topic", 2, 30000, false)
		convey.So(err, convey.ShouldBeNil)
		convey.So(response.Results, convey.ShouldHaveLength, 1)
		convey.So(response.Results[0].TopicName, convey.ShouldEqual, "invalid-topic")
		convey.So(response.Results[0].ErrorCode, convey.ShouldEqual, 37)
		convey.So(response.Results[0].ErrorMessage, convey.ShouldNotBeNil)

		// Test that the response has an error
		responseError := response.Error()
		convey.So(responseError, convey.ShouldNotBeNil)
	})
}

func TestCreatePartitionsValidateOnly(t *testing.T) {
	brokers := &Brokers{}
	c := &Client{
		clientID: "test",
		brokers:  brokers,
	}
	mockey.PatchConvey("TestCreatePartitionsValidateOnly", t, func() {
		mockey.Mock((*Brokers).Controller).Return(int32(1)).Build()
		mockey.Mock((*Brokers).GetBroker).Return(&Broker{}, nil).Build()
		mockey.Mock((*Broker).RequestAndGet).
			To(func(b *Broker, req Request) (Response, error) {
				mockResponse := CreatePartitionsResponse{
					CorrelationID:  1,
					ThrottleTimeMS: 0,
					Results: []createPartitionsResponseResultBlock{
						{
							TopicName:    "validate-topic",
							ErrorCode:    0,
							ErrorMessage: nil,
						},
					},
				}
				return mockResponse, nil
			}).Build()

		// Test partition creation with validate only (dry run)
		response, err := c.CreatePartitions("validate-topic", 8, 30000, true)
		convey.So(err, convey.ShouldBeNil)
		convey.So(response.Results, convey.ShouldHaveLength, 1)
		convey.So(response.Results[0].TopicName, convey.ShouldEqual, "validate-topic")
		convey.So(response.Results[0].ErrorCode, convey.ShouldEqual, 0)
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
