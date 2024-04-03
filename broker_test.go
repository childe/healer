package healer

import (
	"net"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/smartystreets/goconvey/convey"
)

func TestMocknewConn(t *testing.T) {
	mockey.PatchConvey("TestMockXXX", t, func() {
		mockey.Mock((*net.Dialer).Dial).Return(nil, nil).Build() // mock function

		config := &BrokerConfig{
			TLSEnabled: false,
		}
		conn, err := newConn("127.0.0.1:9092", config)
		convey.So(err, convey.ShouldEqual, nil)
		convey.So(conn, convey.ShouldEqual, nil)

		config = &BrokerConfig{
			TLSEnabled: true,
		}
		_, err = newConn("127.0.0.1:9092", config)
		convey.So(err, convey.ShouldEqual, errTLSConfig)
	})
}
