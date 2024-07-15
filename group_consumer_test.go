package healer

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

func TestStopGroupConsumer(t *testing.T) {
	mockey.PatchConvey("stop group consumer", t, func() {
		topic := "testTopic"
		config := map[string]interface{}{
			"bootstrap.servers": "127.0.0.1:9092",
			"group.id":          "healer-test",
		}
		mockey.Mock((*SimpleConsumer).refreshPartiton).Return(nil).Build()
		mockey.Mock((*SimpleConsumer).getLeaderBroker).Return(nil).Build()
		mockey.Mock((*SimpleConsumer).initOffset).Return().Build()
		mockey.Mock((*SimpleConsumer).getCommitedOffet).Return(nil).Build()
		mockey.Mock((*SimpleConsumer).getOffset).Return(0, nil).Build()
		mockey.Mock((*SimpleConsumer).getCoordinator).Return(nil).Build()
		mockey.Mock((*Broker).getHighestAvailableAPIVersion).Return(0).Build()
		mockey.Mock((*Broker).requestLeaveGroup).Return(LeaveGroupResponse{}, nil).Build()
		mockey.Mock(NewBrokersWithConfig).Return(&Brokers{}, nil).Build()
		mockey.Mock((*Brokers).RequestMetaData).Return(MetadataResponse{}, nil).Build()
		mockey.Mock((*Brokers).Close).Return().Build()
		mockey.Mock((*GroupConsumer).joinAndSync).To(func(c *GroupConsumer) error {
			t.Log("mock joinAndSync")
			simpleConsumer, _ := NewSimpleConsumer(topic, 0, config)
			simpleConsumer.belongTO = c
			simpleConsumer.wg = &c.wg
			c.simpleConsumers = append(c.simpleConsumers, simpleConsumer)
			return nil
		}).Build()

		mockey.Mock((*Broker).requestFetchStreamingly).
			To(func(fetchRequest *FetchRequest) (r io.Reader, responseLength uint32, err error) {
				t.Log("mock requestFetchStreamingly")
				return nil, 0, nil
			}).Build()

		mockey.Mock((*fetchResponseStreamDecoder).streamDecode).To(func(decoder *fetchResponseStreamDecoder, ctx context.Context, startOffset int64) error {
			for i := 0; i < 5; i++ {
				select {
				case <-decoder.ctx.Done():
					return nil
				case decoder.messages <- &FullMessage{
					TopicName:   topic,
					PartitionID: 0,
					Error:       nil,
					Message: &Message{
						Offset:      1,
						MessageSize: 10,
						Crc:         1,
						MagicByte:   1,
						Attributes:  1,
						Timestamp:   1,
						Key:         []byte("test"),
						Value:       []byte(fmt.Sprintf("test-%d", i)),
					},
				}:
				}
			}
			return nil
		}).Build()
		consumer, err := NewGroupConsumer(topic, config)
		convey.So(err, convey.ShouldBeNil)

		messages, err := consumer.Consume(nil)

		convey.So(err, convey.ShouldBeNil)
		convey.So(messages, convey.ShouldNotBeNil)

		count := 0
		for count < 10 {
			m := <-messages
			t.Logf("msg: %s", string(m.Message.Value))
			count++
		}
		consumer.Close()
		t.Log("stopped")
	})
}
