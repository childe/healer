package healer

import (
	"net"
	"time"
)

// MockConn is a mock struct for the Conn type
type MockConn struct {
	MockRead             func(p []byte) (n int, err error)
	MockWrite            func(p []byte) (n int, err error)
	MockClose            func() error
	MockLocalAddr        func() net.Addr
	MockRemoteAddr       func() net.Addr
	MockSetDeadline      func(t time.Time) error
	MockSetReadDeadline  func(t time.Time) error
	MockSetWriteDeadline func(t time.Time) error
}

func (_m *MockConn) Read(p []byte) (n int, err error) {
	return len(p), nil
}

func (_m *MockConn) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (_m *MockConn) Close() error {
	return nil
}

func (_m *MockConn) LocalAddr() net.Addr {
	return nil
}

func (_m *MockConn) RemoteAddr() net.Addr {
	return nil
}

func (_m *MockConn) SetDeadline(t time.Time) error {
	return nil
}

func (_m *MockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (_m *MockConn) SetWriteDeadline(t time.Time) error {
	return nil
}
