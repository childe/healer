package healer

import (
	"encoding/binary"
	"fmt"
)

type apiKey int16

func (k apiKey) String() string {
	switch k {
	case 0:
		return "Produce"
	case 1:
		return "Fetch"
	case 2:
		return "ListOffsets"
	case 3:
		return "Metadata"
	case 4:
		return "LeaderAndIsr"
	case 5:
		return "StopReplica"
	case 6:
		return "UpdateMetadata"
	case 7:
		return "ControlledShutdown"
	case 8:
		return "OffsetCommit"
	case 9:
		return "OffsetFetch"
	case 10:
		return "FindCoordinator"
	case 11:
		return "JoinGroup"
	case 12:
		return "Heartbeat"
	case 13:
		return "LeaveGroup"
	case 14:
		return "SyncGroup"
	case 15:
		return "DescribeGroups"
	case 16:
		return "ListGroups"
	case 17:
		return "SaslHandshake"
	case 18:
		return "ApiVersions"
	case 19:
		return "CreateTopics"
	case 20:
		return "DeleteTopics"
	case 21:
		return "DeleteRecords"
	case 22:
		return "InitProducerId"
	case 23:
		return "OffsetForLeaderEpoch"
	case 24:
		return "AddPartitionsToTxn"
	case 25:
		return "AddOffsetsToTxn"
	case 26:
		return "EndTxn"
	case 27:
		return "WriteTxnMarkers"
	case 28:
		return "TxnOffsetCommit"
	case 29:
		return "DescribeAcls"
	case 30:
		return "CreateAcls"
	case 31:
		return "DeleteAcls"
	case 32:
		return "DescribeConfigs"
	case 33:
		return "AlterConfigs"
	case 34:
		return "AlterReplicaLogDirs"
	case 35:
		return "DescribeLogDirs"
	case 36:
		return "SaslAuthenticate"
	case 37:
		return "CreatePartitions"
	case 38:
		return "CreateDelegationToken"
	case 39:
		return "RenewDelegationToken"
	case 40:
		return "ExpireDelegationToken"
	case 41:
		return "DescribeDelegationToken"
	case 42:
		return "DeleteGroups"
	case 43:
		return "ElectLeaders"
	case 44:
		return "IncrementalAlterConfigs"
	case 45:
		return "AlterPartitionReassignments"
	case 46:
		return "ListPartitionReassignments"
	case 47:
		return "OffsetDelete"
	case 48:
		return "DescribeClientQuotas"
	case 49:
		return "AlterClientQuotas"
	case 50:
		return "DescribeUserScramCredentials"
	case 51:
		return "AlterUserScramCredentials"
	case 56:
		return "AlterIsr"
	case 57:
		return "UpdateFeatures"
	case 60:
		return "DescribeCluster"
	case 61:
		return "DescribeProducers"
	case 65:
		return "DescribeTransactions"
	case 66:
		return "ListTransactions"
	case 67:
		return "AllocateProducerIds"
	default:
		return "Unknown"
	}
}

type ApiVersion struct {
	apiKey     apiKey
	minVersion int16
	maxVersion int16
}

// version 0
type ApiVersionsResponse struct {
	CorrelationID uint32
	ErrorCode     uint16
	ApiVersions   []*ApiVersion
}

func NewApiVersionsResponse(payload []byte) (*ApiVersionsResponse, error) {
	apiVersionsResponse := &ApiVersionsResponse{}
	offset := 0
	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return nil, fmt.Errorf("ApiVersions reseponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	apiVersionsResponse.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	apiVersionsResponse.ErrorCode = binary.BigEndian.Uint16(payload[offset:])
	offset += 2

	apiVersionsCount := binary.BigEndian.Uint32(payload[offset:])
	offset += 4
	apiVersionsResponse.ApiVersions = make([]*ApiVersion, apiVersionsCount)

	for i := uint32(0); i < apiVersionsCount; i++ {
		apiVersion := &ApiVersion{}
		apiVersion.apiKey = apiKey(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		apiVersion.minVersion = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		apiVersion.maxVersion = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		apiVersionsResponse.ApiVersions[i] = apiVersion
	}

	return apiVersionsResponse, nil
}
