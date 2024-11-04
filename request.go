package healer

import (
	"encoding/binary"
	"fmt"
)

// TODO type define ApiKey and change api_XXX to ApiKey type

const (
	API_ProduceRequest              uint16 = 0
	API_FetchRequest                uint16 = 1
	API_OffsetRequest               uint16 = 2
	API_MetadataRequest             uint16 = 3
	API_OffsetCommitRequest         uint16 = 8
	API_OffsetFetchRequest          uint16 = 9
	API_FindCoordinator             uint16 = 10
	API_JoinGroup                   uint16 = 11
	API_Heartbeat                   uint16 = 12
	API_LeaveGroup                  uint16 = 13
	API_SyncGroup                   uint16 = 14
	API_DescribeGroups              uint16 = 15
	API_ListGroups                  uint16 = 16
	API_SaslHandshake               uint16 = 17
	API_ApiVersions                 uint16 = 18
	API_CreateTopics                uint16 = 19
	API_DeleteTopics                uint16 = 20
	API_DescribeAcls                uint16 = 29
	API_DescribeConfigs             uint16 = 32
	API_AlterConfigs                uint16 = 33
	API_DescribeLogDirs             uint16 = 35
	API_SaslAuthenticate            uint16 = 36
	API_CreatePartitions            uint16 = 37
	API_Delete_Groups               uint16 = 42
	API_ElectLeaders                uint16 = 43
	API_IncrementalAlterConfigs     uint16 = 44
	API_AlterPartitionReassignments uint16 = 45
	API_ListPartitionReassignments  uint16 = 46
)

// healer only implements these versions of the protocol, only version 0 is supported if not defined here
// It must be sorted from high to low
var availableVersions map[uint16][]uint16 = map[uint16][]uint16{
	API_MetadataRequest:     {7, 4, 1},
	API_FetchRequest:        {10, 7, 0},
	API_OffsetRequest:       {1, 0},
	API_DescribeAcls:        {2, 1, 0},
	API_CreatePartitions:    {2, 0},
	API_SaslHandshake:       {1, 0},
	API_OffsetCommitRequest: {2, 0},
	API_OffsetFetchRequest:  {1, 0},
}

// RequestHeader is the request header, which is used in all requests. It contains apiKey, apiVersion, correlationID, clientID
type RequestHeader struct {
	APIKey        uint16
	APIVersion    uint16
	CorrelationID uint32
	ClientID      *string
	TaggedFields  TaggedFields
}

// not exactly the same as the kafka protocol, but it's enough for now
func (h *RequestHeader) length() int {
	r := 10
	if h.ClientID != nil {
		r += len(*h.ClientID)
	}
	if h.headerVersion() >= 2 {
		if h.TaggedFields != nil {
			r += h.TaggedFields.length()
		} else {
			r++
		}
	}
	return r
}

// Encode encodes request header to []byte. this is used the all request
// If the playload is too small, Encode will panic.
// https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields
// https://kafka.apache.org/protocol#protocol_messages
func (h *RequestHeader) Encode(payload []byte) int {
	offset := 0
	binary.BigEndian.PutUint16(payload[offset:], h.APIKey)
	offset += 2

	binary.BigEndian.PutUint16(payload[offset:], h.APIVersion)
	offset += 2

	binary.BigEndian.PutUint32(payload[offset:], uint32(h.CorrelationID))
	offset += 4

	headerVersion := h.headerVersion()
	switch headerVersion {
	case 1:
		encoded := encodeNullableString(h.ClientID)
		offset += copy(payload[offset:], encoded)
	case 2:
		encoded := encodeNullableString(h.ClientID)
		offset += copy(payload[offset:], encoded)

		tag := h.TaggedFields.Encode()
		offset += copy(payload[offset:], tag)
	}

	return offset
}
func (h *RequestHeader) Read(payload []byte) (n int, err error) {
	n = h.Encode(payload)
	return
}

// DecodeRequestHeader decodes request header from []byte, just used in test cases
func DecodeRequestHeader(payload []byte, version uint16) (h RequestHeader, offset int) {
	h.APIKey = binary.BigEndian.Uint16(payload)
	offset += 2
	h.APIVersion = binary.BigEndian.Uint16(payload[offset:])
	offset += 2

	h.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	headerVersion := h.headerVersion()
	if headerVersion >= 1 {
		clientID, n := nullableString(payload[offset:])
		h.ClientID = clientID
		offset += n
	}
	if headerVersion >= 2 {
		taggedFields, n := DecodeTaggedFields(payload[offset:], version)
		h.TaggedFields = taggedFields
		offset += n
	}

	return
}

// API returns APiKey of the request(which hold the request header)
func (requestHeader *RequestHeader) API() uint16 {
	return requestHeader.APIKey
}

// Version returns API version of the request
func (requestHeader *RequestHeader) Version() uint16 {
	return requestHeader.APIVersion
}

// SetCorrelationID set request's correlationID
func (requestHeader *RequestHeader) SetCorrelationID(c uint32) {
	requestHeader.CorrelationID = c
}

// SetVersion set request's apiversion
func (requestHeader *RequestHeader) SetVersion(version uint16) {
	// if requestHeader.APIVersion == -1 {
	// 	return
	// }
	requestHeader.APIVersion = version
}

// Request is implemented by all detailed request
type Request interface {
	Encode(version uint16) []byte
	API() uint16
	SetCorrelationID(uint32)
	SetVersion(uint16)
}

// https://github.com/apache/kafka/tree/trunk/clients/src/main/resources/common/message
func (h *RequestHeader) headerVersion() uint16 {
	_version := h.APIVersion
	switch h.APIKey {
	case 0: // Produce
		if _version >= 9 {
			return 2
		} else {
			return 1
		}
	case 1: // Fetch
		if _version >= 12 {
			return 2
		} else {
			return 1
		}
	case 2: // ListOffsets
		if _version >= 6 {
			return 2
		} else {
			return 1
		}
	case 3: // Metadata
		if _version >= 9 {
			return 2
		} else {
			return 1
		}
	case 4: // LeaderAndIsr
		if _version >= 4 {
			return 2
		} else {
			return 1
		}
	case 5: // StopReplica
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 6: // UpdateMetadata
		if _version >= 6 {
			return 2
		} else {
			return 1
		}
	case 7: // ControlledShutdown
		// Version 0 of ControlledShutdownRequest has a non-standard request header
		// which does not include clientId.  Version 1 of ControlledShutdownRequest
		// and later use the standard request header.
		if _version == 0 {
			return 0
		}
		if _version >= 3 {
			return 2
		} else {
			return 1
		}
	case 8: // OffsetCommit
		if _version >= 8 {
			return 2
		} else {
			return 1
		}
	case 9: // OffsetFetch
		if _version >= 6 {
			return 2
		} else {
			return 1
		}
	case 10: // FindCoordinator
		if _version >= 3 {
			return 2
		} else {
			return 1
		}
	case 11: // JoinGroup
		if _version >= 6 {
			return 2
		} else {
			return 1
		}
	case 12: // Heartbeat
		if _version >= 4 {
			return 2
		} else {
			return 1
		}
	case 13: // LeaveGroup
		if _version >= 4 {
			return 2
		} else {
			return 1
		}
	case 14: // SyncGroup
		if _version >= 4 {
			return 2
		} else {
			return 1
		}
	case 15: // DescribeGroups
		if _version >= 5 {
			return 2
		} else {
			return 1
		}
	case 16: // ListGroups
		if _version >= 3 {
			return 2
		} else {
			return 1
		}
	case 17: // SaslHandshake
		return 1
	case 18: // ApiVersions
		if _version >= 3 {
			return 2
		} else {
			return 1
		}
	case 19: // CreateTopics
		if _version >= 5 {
			return 2
		} else {
			return 1
		}
	case 20: // DeleteTopics
		if _version >= 4 {
			return 2
		} else {
			return 1
		}
	case 21: // DeleteRecords
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 22: // InitProducerId
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 23: // OffsetForLeaderEpoch
		if _version >= 4 {
			return 2
		} else {
			return 1
		}
	case 24: // AddPartitionsToTxn
		if _version >= 3 {
			return 2
		} else {
			return 1
		}
	case 25: // AddOffsetsToTxn
		if _version >= 3 {
			return 2
		} else {
			return 1
		}
	case 26: // EndTxn
		if _version >= 3 {
			return 2
		} else {
			return 1
		}
	case 27: // WriteTxnMarkers
		if _version >= 1 {
			return 2
		} else {
			return 1
		}
	case 28: // TxnOffsetCommit
		if _version >= 3 {
			return 2
		} else {
			return 1
		}
	case 29: // DescribeAcls
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 30: // CreateAcls
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 31: // DeleteAcls
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 32: // DescribeConfigs
		if _version >= 4 {
			return 2
		} else {
			return 1
		}
	case 33: // AlterConfigs
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 34: // AlterReplicaLogDirs
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 35: // DescribeLogDirs
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 36: // SaslAuthenticate
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 37: // CreatePartitions
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 38: // CreateDelegationToken
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 39: // RenewDelegationToken
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 40: // ExpireDelegationToken
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 41: // DescribeDelegationToken
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 42: // DeleteGroups
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 43: // ElectLeaders
		if _version >= 2 {
			return 2
		} else {
			return 1
		}
	case 44: // IncrementalAlterConfigs
		if _version >= 1 {
			return 2
		} else {
			return 1
		}
	case 45: // AlterPartitionReassignments
		return 2
	case 46: // ListPartitionReassignments
		return 2
	case 47: // OffsetDelete
		return 1
	case 48: // DescribeClientQuotas
		if _version >= 1 {
			return 2
		} else {
			return 1
		}
	case 49: // AlterClientQuotas
		if _version >= 1 {
			return 2
		} else {
			return 1
		}
	case 50: // DescribeUserScramCredentials
		return 2
	case 51: // AlterUserScramCredentials
		return 2
	case 52: // Vote
		return 2
	case 53: // BeginQuorumEpoch
		return 1
	case 54: // EndQuorumEpoch
		return 1
	case 55: // DescribeQuorum
		return 2
	case 56: // AlterPartition
		return 2
	case 57: // UpdateFeatures
		return 2
	case 58: // Envelope
		return 2
	case 59: // FetchSnapshot
		return 2
	case 60: // DescribeCluster
		return 2
	case 61: // DescribeProducers
		return 2
	case 62: // BrokerRegistration
		return 2
	case 63: // BrokerHeartbeat
		return 2
	case 64: // UnregisterBroker
		return 2
	case 65: // DescribeTransactions
		return 2
	case 66: // ListTransactions
		return 2
	case 67: // AllocateProducerIds
		return 2
	default:
		panic(fmt.Sprintf("Unsupported API key %v", h.APIKey))
	}
}
