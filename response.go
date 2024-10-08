package healer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// Response is the interface of all response. Error() returns the error abstracted from the error code of the response
type Response interface {
	Error() error
}

// ReadParser read data from a connection of broker and parse the response
type ReadParser interface {
	Read() ([]byte, error)
	Parse(data []byte) (Response, error)
	ReadAndParse() (Response, error)
}

type defaultReadParser struct {
	broker  *Broker
	api     uint16
	version uint16
	timeout int
}

// ReadAndParse read a whole response data from broker and parse it
func (p defaultReadParser) ReadAndParse() (Response, error) {
	data, err := p.Read()
	if err != nil {
		return nil, fmt.Errorf("read response of %d(%d) from %s error: %w", p.api, p.version, p.broker.GetAddress(), err)
	}

	resp, err := p.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("parse response of %d(%d) from %s error: %w", p.api, p.version, p.broker.GetAddress(), err)
	}
	return resp, nil
}

// Read read a whole response data from broker. it firstly read length of the response data, then read the whole response data
func (p defaultReadParser) Read() ([]byte, error) {
	//TODO use LimitedReader
	l := 0
	responseLengthBuf := make([]byte, 4)
	for {
		if p.timeout > 0 {
			p.broker.conn.SetReadDeadline(time.Now().Add(time.Duration(p.timeout) * time.Millisecond))
		}
		length, err := p.broker.conn.Read(responseLengthBuf[l:])
		if err != nil {
			return nil, err
		}

		if length+l == 4 {
			break
		}
		l += length
	}
	responseLength := int(binary.BigEndian.Uint32(responseLengthBuf))
	resp := make([]byte, 4+responseLength)

	readLength := 0
	for {
		if p.timeout > 0 {
			p.broker.conn.SetReadDeadline(time.Now().Add(time.Duration(p.timeout) * time.Millisecond))
		}
		length, err := p.broker.conn.Read(resp[4+readLength:])
		if err != nil {
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
	// logger.V(5).Info("response info", "length", len(resp), "CorrelationID", binary.BigEndian.Uint32(resp[4:]))
	return resp, nil
}

func (p defaultReadParser) Parse(data []byte) (Response, error) {
	switch p.api {
	case API_Heartbeat:
		return NewHeartbeatResponse(data)
	case API_ProduceRequest:
		return NewProduceResponse(data)
	case API_MetadataRequest:
		return NewMetadataResponse(data, p.version)
	case API_ApiVersions:
		return newAPIVersionsResponse(data)
	case API_SaslHandshake:
		return NewSaslHandshakeResponse(data)
	case API_SaslAuthenticate:
		return NewSaslAuthenticateResponse(data)
	case API_OffsetRequest:
		return NewOffsetsResponse(data, p.version)
	case API_OffsetFetchRequest:
		return NewOffsetFetchResponse(data)
	case API_FindCoordinator:
		return NewFindCoordinatorResponse(data, p.version)
	case API_JoinGroup:
		return NewJoinGroupResponse(data)
	case API_LeaveGroup:
		return NewLeaveGroupResponse(data)
	case API_OffsetCommitRequest:
		return NewOffsetCommitResponse(data)
	case API_DescribeGroups:
		return NewDescribeGroupsResponse(data)
	case API_SyncGroup:
		return NewSyncGroupResponse(data)
	case API_DescribeConfigs:
		return NewDescribeConfigsResponse(data)
	case API_AlterPartitionReassignments:
		return NewAlterPartitionReassignmentsResponse(data, p.version)
	case API_ListPartitionReassignments:
		return NewListPartitionReassignmentsResponse(data, p.version)
	case API_ListGroups:
		return NewListGroupsResponse(data)
	case API_CreateTopics:
		return NewCreateTopicsResponse(data)
	case API_DeleteTopics:
		return NewDeleteTopicsResponse(data, p.version)
	case API_AlterConfigs:
		return NewAlterConfigsResponse(data)
	case API_Delete_Groups:
		return NewDeleteGroupsResponse(data)
	case API_IncrementalAlterConfigs:
		return NewIncrementalAlterConfigsResponse(data, p.version)
	case API_CreatePartitions:
		return NewCreatePartitionsResponse(data, p.version)
	case API_DescribeLogDirs:
		return NewDescribeLogDirsResponse(data, p.version)
	case API_ElectLeaders:
		return NewElectLeadersResponse(data, p.version)
	}
	return nil, errors.New("unknown api")
}
