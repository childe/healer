package healer

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

func TestCreateConsumerConfig(t *testing.T) {
	mockey.PatchConvey("TestNil", t, func() {
		concumerConfig, err := createConsumerConfig(nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(concumerConfig, convey.ShouldResemble, defaultConsumerConfig)

		convey.So(defaultConsumerConfig.BootstrapServers, convey.ShouldEqual, "")
	})
	mockey.PatchConvey("TestMap", t, func() {
		configMap := map[string]interface{}{
			"bootstrap.servers": "localhost:9092",
			"group.id":          "test",
			"mechanism":         "plain",
			"sasl": map[string]interface{}{
				"mechanism": "plain",
			},
		}
		concumerConfig, err := createConsumerConfig(configMap)
		convey.So(err, convey.ShouldBeNil)

		convey.So(concumerConfig.BootstrapServers, convey.ShouldEqual, "localhost:9092")
		convey.So(concumerConfig.GroupID, convey.ShouldEqual, "test")
		convey.So(concumerConfig.RetryBackOffMS, convey.ShouldEqual, 100)
		convey.So(concumerConfig.Sasl.Mechanism, convey.ShouldEqual, "plain")

		convey.So(defaultConsumerConfig.BootstrapServers, convey.ShouldEqual, "")
	})
	mockey.PatchConvey("TestStruct", t, func() {
		configMap := ConsumerConfig{
			BootstrapServers: "localhost:9092",
		}
		concumerConfig, err := createConsumerConfig(configMap)
		convey.So(err, convey.ShouldBeNil)
		convey.So(concumerConfig.BootstrapServers, convey.ShouldEqual, "localhost:9092")
		convey.So(concumerConfig.GroupID, convey.ShouldEqual, "")
		convey.So(concumerConfig.RetryBackOffMS, convey.ShouldEqual, 0)

		convey.So(defaultConsumerConfig.BootstrapServers, convey.ShouldEqual, "")
	})
	mockey.PatchConvey("TestTypeDefault", t, func() {
		configMap := make(map[string]string)
		concumerConfig, err := createConsumerConfig(configMap)
		convey.So(err.Error(), convey.ShouldEqual, "consumer only accept config from map[string]interface{} or ConsumerConfig")
		convey.So(concumerConfig.BootstrapServers, convey.ShouldEqual, "")

		convey.So(defaultConsumerConfig.BootstrapServers, convey.ShouldEqual, "")
	})

	mockey.PatchConvey("test timeout.ms.for.eachapi based on net.timeout.ms", t, func() {
		configMap := make(map[string]interface{})
		configMap["net"] = map[string]string{
			"timeout.ms": "60000",
		}
		concumerConfig, err := createConsumerConfig(configMap)
		convey.So(err, convey.ShouldBeNil)

		convey.So(concumerConfig.Net.TimeoutMSForEachAPI[0], convey.ShouldEqual, 60000)
	})
}
