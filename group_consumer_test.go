package healer

import (
	"context"
	"fmt"
	"io"
	"testing"

	"errors"

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

func TestGroupConsumerJoinError(t *testing.T) {
	mockey.PatchConvey("test group consumer join error handling", t, func() {
		topic := "testTopic"
		config := map[string]interface{}{
			"bootstrap.servers":  "127.0.0.1:9092",
			"group.id":           "healer-test",
			"client.id":          "test-client",
			"session.timeout.ms": 30000,
		}

		// 创建 GroupConsumer 实例
		mockey.Mock(NewBrokersWithConfig).Return(&Brokers{}, nil).Build()
		consumer, err := NewGroupConsumer(topic, config)
		convey.So(err, convey.ShouldBeNil)

		// 模拟 coordinator
		mockBroker := &Broker{}
		consumer.coordinator = mockBroker
		consumer.coordinatorAvailable = true
		consumer.memberID = "test-member-id"

		// 测试场景 1: KafkaError(25) - UNKNOWN_MEMBER_ID
		// 应该设置 memberID 为空
		convey.Convey("When coordinator returns UNKNOWN_MEMBER_ID error", func() {
			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, KafkaError(25)).Build()

			err := consumer.join()

			convey.So(err, convey.ShouldEqual, KafkaError(25))
			convey.So(consumer.memberID, convey.ShouldEqual, "")
		})

		// 测试场景 2: io.EOF 错误
		// 应该设置 coordinatorAvailable 为 false
		convey.Convey("When coordinator returns io.EOF error", func() {
			consumer.coordinatorAvailable = true
			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, io.EOF).Build()

			err := consumer.join()

			convey.So(err, convey.ShouldEqual, io.EOF)
			convey.So(consumer.coordinatorAvailable, convey.ShouldBeFalse)
		})

		// 测试场景 3: KafkaError(15) - COORDINATOR_NOT_AVAILABLE
		// 应该设置 coordinatorAvailable 为 false
		convey.Convey("When coordinator returns COORDINATOR_NOT_AVAILABLE error", func() {
			consumer.coordinatorAvailable = true
			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, KafkaError(15)).Build()

			err := consumer.join()

			convey.So(err, convey.ShouldEqual, KafkaError(15))
			convey.So(consumer.coordinatorAvailable, convey.ShouldBeFalse)
		})

		// 测试场景 4: KafkaError(16) - NOT_COORDINATOR
		// 应该设置 coordinatorAvailable 为 false
		convey.Convey("When coordinator returns NOT_COORDINATOR error", func() {
			consumer.coordinatorAvailable = true
			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, KafkaError(16)).Build()

			err := consumer.join()

			convey.So(err, convey.ShouldEqual, KafkaError(16))
			convey.So(consumer.coordinatorAvailable, convey.ShouldBeFalse)
		})

		// 测试场景 5: "connection refused" 错误
		// 应该设置 coordinatorAvailable 为 false
		convey.Convey("When coordinator returns connection refused error", func() {
			consumer.coordinatorAvailable = true
			connectionError := fmt.Errorf("connection refused")
			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, connectionError).Build()

			err := consumer.join()

			convey.So(err, convey.ShouldEqual, connectionError)
			convey.So(consumer.coordinatorAvailable, convey.ShouldBeFalse)
		})

		// 测试场景 6: "network is unreachable" 错误
		// 应该设置 coordinatorAvailable 为 false
		convey.Convey("When coordinator returns network unreachable error", func() {
			consumer.coordinatorAvailable = true
			networkError := fmt.Errorf("network is unreachable")
			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, networkError).Build()

			err := consumer.join()

			convey.So(err, convey.ShouldEqual, networkError)
			convey.So(consumer.coordinatorAvailable, convey.ShouldBeFalse)
		})

		// 测试场景 7: "i/o timeout" 错误
		// 应该设置 coordinatorAvailable 为 false
		convey.Convey("When coordinator returns i/o timeout error", func() {
			consumer.coordinatorAvailable = true
			timeoutError := fmt.Errorf("i/o timeout")
			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, timeoutError).Build()

			err := consumer.join()

			convey.So(err, convey.ShouldEqual, timeoutError)
			convey.So(consumer.coordinatorAvailable, convey.ShouldBeFalse)
		})

		// 测试场景 8: 成功场景 - 作为 leader
		// 应该设置正确的 generationID, memberID 和 ifLeader
		convey.Convey("When join succeeds as leader", func() {
			consumer.coordinatorAvailable = true
			consumer.memberID = ""
			successResponse := JoinGroupResponse{
				GenerationID: 5,
				MemberID:     "new-member-id",
				LeaderID:     "new-member-id",
				Members: []Member{
					{MemberID: "new-member-id", MemberMetadata: []byte("test-metadata")},
				},
			}
			mockey.Mock((*Broker).requestJoinGroup).Return(successResponse, nil).Build()

			err := consumer.join()

			convey.So(err, convey.ShouldBeNil)
			convey.So(consumer.generationID, convey.ShouldEqual, 5)
			convey.So(consumer.memberID, convey.ShouldEqual, "new-member-id")
			convey.So(consumer.ifLeader, convey.ShouldBeTrue)
			convey.So(len(consumer.members), convey.ShouldEqual, 1)
		})

		// 测试场景 9: 成功场景 - 作为 follower
		// 应该设置正确的 generationID, memberID 和 ifLeader
		convey.Convey("When join succeeds as follower", func() {
			consumer.coordinatorAvailable = true
			consumer.memberID = ""
			successResponse := JoinGroupResponse{
				GenerationID: 3,
				MemberID:     "follower-member-id",
				LeaderID:     "leader-member-id",
				Members: []Member{
					{MemberID: "leader-member-id", MemberMetadata: []byte("leader-metadata")},
					{MemberID: "follower-member-id", MemberMetadata: []byte("follower-metadata")},
				},
			}
			mockey.Mock((*Broker).requestJoinGroup).Return(successResponse, nil).Build()

			err := consumer.join()

			convey.So(err, convey.ShouldBeNil)
			convey.So(consumer.generationID, convey.ShouldEqual, 3)
			convey.So(consumer.memberID, convey.ShouldEqual, "follower-member-id")
			convey.So(consumer.ifLeader, convey.ShouldBeFalse)
			convey.So(len(consumer.members), convey.ShouldEqual, 0) // follower 不会设置 members
		})
	})
}

func TestGroupConsumerJoinWrappedError(t *testing.T) {
	mockey.PatchConvey("test group consumer join wrapped error handling", t, func() {
		topic := "testTopic"
		config := map[string]interface{}{
			"bootstrap.servers":  "127.0.0.1:9092",
			"group.id":           "healer-test",
			"client.id":          "test-client",
			"session.timeout.ms": 30000,
		}

		// 创建 GroupConsumer 实例
		mockey.Mock(NewBrokersWithConfig).Return(&Brokers{}, nil).Build()
		consumer, err := NewGroupConsumer(topic, config)
		convey.So(err, convey.ShouldBeNil)

		// 模拟 coordinator
		mockBroker := &Broker{address: "127.0.0.1:9092"}
		consumer.coordinator = mockBroker
		consumer.coordinatorAvailable = true
		consumer.memberID = "test-member-id"

		// 测试场景：模拟 ReadAndParse 中 Parse 返回 KafkaError(25)，然后被包装
		convey.Convey("When ReadAndParse wraps KafkaError(25) in Parse", func() {
			// 创建一个包装过的错误，模拟 ReadAndParse 的行为
			wrappedError := fmt.Errorf("parse response of %d(%d) from %s error: %w",
				11, 1, "127.0.0.1:9092", KafkaError(25))

			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, wrappedError).Build()

			err := consumer.join()

			// 验证错误能够正确返回
			convey.So(err, convey.ShouldEqual, wrappedError)

			// 验证当前的 == 比较不能识别包装过的错误
			convey.So(err == KafkaError(25), convey.ShouldBeFalse)

			// 验证 errors.Is 可以识别包装过的错误
			convey.So(errors.Is(err, KafkaError(25)), convey.ShouldBeTrue)

			// 验证修复后的代码能正确处理包装过的错误，memberID 被清空
			convey.So(consumer.memberID, convey.ShouldEqual, "")
		})

		// 测试场景：模拟其他被包装的 KafkaError
		convey.Convey("When ReadAndParse wraps KafkaError(15) in Parse", func() {
			consumer.coordinatorAvailable = true
			wrappedError := fmt.Errorf("parse response of %d(%d) from %s error: %w",
				11, 1, "127.0.0.1:9092", KafkaError(15))

			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, wrappedError).Build()

			err := consumer.join()

			convey.So(err, convey.ShouldEqual, wrappedError)
			convey.So(err == KafkaError(15), convey.ShouldBeFalse)
			convey.So(errors.Is(err, KafkaError(15)), convey.ShouldBeTrue)

			// 验证修复后的代码能正确处理包装过的错误，coordinatorAvailable 被设置为 false
			convey.So(consumer.coordinatorAvailable, convey.ShouldBeFalse)
		})

		// 测试场景：模拟 joinAndSync 中的错误比较
		convey.Convey("When joinAndSync checks wrapped KafkaError(25)", func() {
			consumer.coordinatorAvailable = true
			wrappedError := fmt.Errorf("parse response of %d(%d) from %s error: %w",
				11, 1, "127.0.0.1:9092", KafkaError(25))

			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, wrappedError).Build()

			// 模拟 joinAndSync 中的错误检查逻辑
			err := consumer.join()

			// 验证当前的 == 比较在 joinAndSync 中也会失败
			if err == KafkaError(22) || err == KafkaError(25) || err == KafkaError(27) {
				t.Error("不应该进入这个分支，因为包装过的错误不会等于 KafkaError(25)")
			}

			// 验证 errors.Is 可以正确识别
			convey.So(errors.Is(err, KafkaError(25)), convey.ShouldBeTrue)
		})
	})
}

func TestGroupConsumerJoinWrappedErrorFixed(t *testing.T) {
	mockey.PatchConvey("test group consumer join wrapped error handling after fix", t, func() {
		topic := "testTopic"
		config := map[string]interface{}{
			"bootstrap.servers":  "127.0.0.1:9092",
			"group.id":           "healer-test",
			"client.id":          "test-client",
			"session.timeout.ms": 30000,
		}

		// 创建 GroupConsumer 实例
		mockey.Mock(NewBrokersWithConfig).Return(&Brokers{}, nil).Build()
		consumer, err := NewGroupConsumer(topic, config)
		convey.So(err, convey.ShouldBeNil)

		// 模拟 coordinator
		mockBroker := &Broker{address: "127.0.0.1:9092"}
		consumer.coordinator = mockBroker
		consumer.coordinatorAvailable = true
		consumer.memberID = "test-member-id"

		// 测试场景：验证修复后的代码能正确处理包装的 KafkaError(25)
		convey.Convey("When wrapped KafkaError(25) is handled correctly after fix", func() {
			// 创建一个包装过的错误，模拟 ReadAndParse 的行为
			wrappedError := fmt.Errorf("parse response of %d(%d) from %s error: %w",
				11, 1, "127.0.0.1:9092", KafkaError(25))

			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, wrappedError).Build()

			err := consumer.join()

			// 验证错误能够正确返回
			convey.So(err, convey.ShouldEqual, wrappedError)

			// 验证修复后的 errors.Is 能够正确识别包装过的错误，并清空 memberID
			convey.So(consumer.memberID, convey.ShouldEqual, "")
		})

		// 测试场景：验证修复后的代码能正确处理包装的 KafkaError(15)
		convey.Convey("When wrapped KafkaError(15) is handled correctly after fix", func() {
			consumer.coordinatorAvailable = true
			consumer.memberID = "test-member-id"
			wrappedError := fmt.Errorf("parse response of %d(%d) from %s error: %w",
				11, 1, "127.0.0.1:9092", KafkaError(15))

			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, wrappedError).Build()

			err := consumer.join()

			convey.So(err, convey.ShouldEqual, wrappedError)

			// 验证修复后的 errors.Is 能够正确识别包装过的错误，并设置 coordinatorAvailable
			convey.So(consumer.coordinatorAvailable, convey.ShouldBeFalse)
		})

		// 测试场景：验证修复后的代码能正确处理包装的 KafkaError(16)
		convey.Convey("When wrapped KafkaError(16) is handled correctly after fix", func() {
			consumer.coordinatorAvailable = true
			consumer.memberID = "test-member-id"
			wrappedError := fmt.Errorf("parse response of %d(%d) from %s error: %w",
				11, 1, "127.0.0.1:9092", KafkaError(16))

			mockey.Mock((*Broker).requestJoinGroup).Return(JoinGroupResponse{}, wrappedError).Build()

			err := consumer.join()

			convey.So(err, convey.ShouldEqual, wrappedError)

			// 验证修复后的 errors.Is 能够正确识别包装过的错误，并设置 coordinatorAvailable
			convey.So(consumer.coordinatorAvailable, convey.ShouldBeFalse)
		})
	})
}
