package healer

import (
	"encoding/binary"
	"net"
	"time"
)

// mockConn is a mock struct for the Conn type
type mockConn struct {
	MockRead             func(p []byte) (n int, err error)
	MockWrite            func(p []byte) (n int, err error)
	MockClose            func() error
	MockLocalAddr        func() net.Addr
	MockRemoteAddr       func() net.Addr
	MockSetDeadline      func(t time.Time) error
	MockSetReadDeadline  func(t time.Time) error
	MockSetWriteDeadline func(t time.Time) error
}

func (_m *mockConn) Read(p []byte) (n int, err error) {
	// mock the response length
	if len(p) == 4 {
		binary.BigEndian.PutUint32(p, 100)
	}

	return len(p), nil
}

func (_m *mockConn) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (_m *mockConn) Close() error {
	return nil
}

func (_m *mockConn) LocalAddr() net.Addr {
	return nil
}

func (_m *mockConn) RemoteAddr() net.Addr {
	return nil
}

func (_m *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (_m *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (_m *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}
