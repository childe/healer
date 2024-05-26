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
