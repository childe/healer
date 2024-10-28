package healer

import (
	"bytes"
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"k8s.io/klog/v2"
)

func BenchmarkSimpleConsumer(b *testing.B) {
	mockey.PatchConvey("benchmark simple consumer", b, func() {
		SetLogger(klog.NewKlogr().WithName("healer-test").WithSink(nil))

		topic := "testTopic"
		partitionID := 1
		config := map[string]interface{}{
			"bootstrap.servers": "localhost:9092",
			"client.id":         "healer-benchmark",
		}
		mockey.Mock(NewBrokersWithConfig).Return(&Brokers{
			brokersInfo: map[int32]*BrokerInfo{
				1: {
					NodeID: 0,
					Host:   "localhost",
					Port:   9092,
				},
			},
		}, nil).Build()
		var version uint16 = 10
		mockey.Mock((*SimpleConsumer).refreshPartiton).Return(nil).Build()
		mockey.Mock((*SimpleConsumer).getLeaderBroker).Return(nil).Build()
		mockey.Mock((*SimpleConsumer).initOffset).Return().Build()
		mockey.Mock((*SimpleConsumer).getOffset).Return(0, nil).Build()
		mockey.Mock((*Broker).getHighestAvailableAPIVersion).Return(version).Build()
		payload, _ := resp.Encode(version)
		mockey.Mock((*Broker).requestFetchStreamingly).To(func(fetchRequest *FetchRequest) (io.Reader, uint32, error) {
			reader := bytes.NewReader(payload)
			return reader, uint32(len(payload)), nil
		}).Build()
		mockey.Mock((*fetchResponseStreamDecoder).filterMessage).Return(true).Build()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			simpleConsumer, _ := NewSimpleConsumer(topic, int32(partitionID), config)
			messages, _ := simpleConsumer.Consume(-1, nil)
			for i := 0; i < 1000; i++ {
				<-messages
			}
			simpleConsumer.Stop()
		}
	})
}
