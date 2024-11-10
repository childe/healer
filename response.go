package healer

import (
	"bytes"
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
	case API_DescribeAcls:
		return NewDescribeAclsResponse(data, p.version)
	case API_CreateAcls:
		return DecodeCreateAclsResponse(data, p.version)
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

type ResponseHeader struct {
	apiKey        uint16
	apiVersion    uint16
	headerVersion uint16
	CorrelationID uint32
	TaggedFields  TaggedFields // version +1
}

func NewResponseHeader(apiKey, apiVersion uint16) ResponseHeader {
	return ResponseHeader{
		apiKey:        apiKey,
		apiVersion:    apiVersion,
		headerVersion: responseHeaderVersion(apiKey, apiVersion),
	}
}

// not exactly, but enough
func (h *ResponseHeader) length() (n int) {
	n = 4
	if h.IsFlexible() {
		n += h.TaggedFields.length()
	}
	return n
}

func (h *ResponseHeader) Encode() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, h.TaggedFields.length()))
	binary.Write(buf, binary.BigEndian, h.CorrelationID)
	if h.IsFlexible() {
		buf.Write(h.TaggedFields.Encode())
	}
	return buf.Bytes()
}
func (h *ResponseHeader) EncodeTo(payload []byte) (offset int) {
	binary.BigEndian.PutUint32(payload[offset:], h.CorrelationID)
	offset += 4
	if h.IsFlexible() {
		offset += copy(payload[offset:], h.TaggedFields.Encode())
	}
	return offset
}

func DecodeResponseHeader(payload []byte, apiKey uint16, apiVersion uint16) (header ResponseHeader, offset int) {
	header = NewResponseHeader(apiKey, apiVersion)
	header.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4
	if header.IsFlexible() {
		taggedFields, n := DecodeTaggedFields(payload[offset:])
		offset += n
		header.TaggedFields = taggedFields
	}
	return header, offset
}
func (h *ResponseHeader) IsFlexible() bool {
	return h.headerVersion >= 1
}

// https://kafka.apache.org/protocol#protocol_api_keys
// Response Header v0 => correlation_id
// Response Header v1 => correlation_id TAG_BUFFER
func responseHeaderVersion(apiKey uint16, apiVersion uint16) uint16 {
	switch apiKey {
	case 0: // Produce
		if apiVersion >= 9 {
			return 1
		} else {
			return 0
		}
	case 1: // Fetch
		if apiVersion >= 12 {
			return 1
		} else {
			return 0
		}
	case 2: // ListOffsets
		if apiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 3: // Metadata
		if apiVersion >= 9 {
			return 1
		} else {
			return 0
		}
	case 4: // LeaderAndIsr
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 5: // StopReplica
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 6: // UpdateMetadata
		if apiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 7: // ControlledShutdown
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 8: // OffsetCommit
		if apiVersion >= 8 {
			return 1
		} else {
			return 0
		}
	case 9: // OffsetFetch
		if apiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 10: // FindCoordinator
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 11: // JoinGroup
		if apiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 12: // Heartbeat
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 13: // LeaveGroup
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 14: // SyncGroup
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 15: // DescribeGroups
		if apiVersion >= 5 {
			return 1
		} else {
			return 0
		}
	case 16: // ListGroups
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 17: // SaslHandshake
		return 0
	case 18: // ApiVersions
		// ApiVersionsResponse always includes a v0 header.
		// See KIP-511 for details.
		return 0
	case 19: // CreateTopics
		if apiVersion >= 5 {
			return 1
		} else {
			return 0
		}
	case 20: // DeleteTopics
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 21: // DeleteRecords
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 22: // InitProducerId
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 23: // OffsetForLeaderEpoch
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 24: // AddPartitionsToTxn
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 25: // AddOffsetsToTxn
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 26: // EndTxn
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 27: // WriteTxnMarkers
		if apiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 28: // TxnOffsetCommit
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 29: // DescribeAcls
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 30: // CreateAcls
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 31: // DeleteAcls
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 32: // DescribeConfigs
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 33: // AlterConfigs
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 34: // AlterReplicaLogDirs
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 35: // DescribeLogDirs
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 36: // SaslAuthenticate
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 37: // CreatePartitions
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 38: // CreateDelegationToken
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 39: // RenewDelegationToken
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 40: // ExpireDelegationToken
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 41: // DescribeDelegationToken
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 42: // DeleteGroups
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 43: // ElectLeaders
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 44: // IncrementalAlterConfigs
		if apiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 45: // AlterPartitionReassignments
		return 1
	case 46: // ListPartitionReassignments
		return 1
	case 47: // OffsetDelete
		return 0
	case 48: // DescribeClientQuotas
		if apiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 49: // AlterClientQuotas
		if apiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 50: // DescribeUserScramCredentials
		return 1
	case 51: // AlterUserScramCredentials
		return 1
	case 52: // Vote
		return 1
	case 53: // BeginQuorumEpoch
		return 0
	case 54: // EndQuorumEpoch
		return 0
	case 55: // DescribeQuorum
		return 1
	case 56: // AlterPartition
		return 1
	case 57: // UpdateFeatures
		return 1
	case 58: // Envelope
		return 1
	case 59: // FetchSnapshot
		return 1
	case 60: // DescribeCluster
		return 1
	case 61: // DescribeProducers
		return 1
	case 62: // BrokerRegistration
		return 1
	case 63: // BrokerHeartbeat
		return 1
	case 64: // UnregisterBroker
		return 1
	case 65: // DescribeTransactions
		return 1
	case 66: // ListTransactions
		return 1
	case 67: // AllocateProducerIds
		return 1
	default:
		panic(fmt.Sprintf("Unsupported API key %v", apiKey))
	}
}
