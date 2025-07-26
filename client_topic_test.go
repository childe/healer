package healer

import (
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
