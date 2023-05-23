package healer

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/golang/glog"
)

type response interface {
	Error() error
}

// readParser read data from a connection of broker and parse the response
type readParser interface {
	Read() ([]byte, error)
	Parse(data []byte) (response, error)
}

type defaultReadParser struct {
	broker  Broker
	api     uint16
	version uint16
	timeout int
}

// Read read a whole response data from broker. it firstly read length of the response data, then read the whole response data
func (p defaultReadParser) Read() ([]byte, error) {
	l := 0
	responseLengthBuf := make([]byte, 4)
	for {
		if p.timeout > 0 {
			p.broker.conn.SetReadDeadline(time.Now().Add(time.Duration(p.timeout) * time.Millisecond))
		}
		length, err := p.broker.conn.Read(responseLengthBuf[l:])
		if err != nil {
			p.broker.Close()
			return nil, err
		}

		if length+l == 4 {
			break
		}
		l += length
	}
	responseLength := int(binary.BigEndian.Uint32(responseLengthBuf))
	glog.V(10).Infof("response length in header: %d", responseLength+4)
	resp := make([]byte, 4+responseLength)

	readLength := 0
	for {
		if p.timeout > 0 {
			p.broker.conn.SetReadDeadline(time.Now().Add(time.Duration(p.timeout) * time.Millisecond))
		}
		length, err := p.broker.conn.Read(resp[4+readLength:])
		if err != nil {
			p.broker.Close()
			return nil, err
		}

		readLength += length
		if readLength > responseLength {
			return nil, errors.New("fetch more data than needed while read response")
		}
		if readLength == responseLength {
			break
		}
	}
	copy(resp[0:4], responseLengthBuf)
	if glog.V(10) {
		glog.Infof("response length: %d. CorrelationID: %d", len(resp), binary.BigEndian.Uint32(resp[4:]))
	}
	return resp, nil
}

func (p defaultReadParser) Parse(data []byte) (response, error) {
	switch p.api {
	case API_ApiVersions:
		return NewApiVersionsResponse(data)
	}
	return nil, errors.New("unknown api")
}
