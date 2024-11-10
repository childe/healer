package healer

import (
	"fmt"
	"strings"
)

// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/resource/ResourceType.java
type AclsResourceType int8

const (
	AclsResourceTypeUnknown         = 0
	AclsResourceTypeAny             = 1
	AclsResourceTypeTopic           = 2
	AclsResourceTypeGroup           = 3
	AclsResourceTypeBroker          = 4
	AclsResourceTypeCluster         = 4
	AclsResourceTypeTransactionalID = 5
	AclsResourceTypeDelegationToken = 6
	AclsResourceTypeUser            = 7
)

func (t AclsResourceType) String() string {
	switch t {
	case AclsResourceTypeUnknown:
		return "UNKNOWN"
	case AclsResourceTypeAny:
		return "ANY"
	case AclsResourceTypeTopic:
		return "TOPIC"
	case AclsResourceTypeGroup:
		return "GROUP"
	case AclsResourceTypeBroker:
		return "BROKER"
	case AclsResourceTypeTransactionalID:
		return "TRANSACTIONAL_ID"
	case AclsResourceTypeDelegationToken:
		return "DELEGATION_TOKEN"
	case AclsResourceTypeUser:
		return "USER"
	default:
		return "ERROR"
	}
}
func (t AclsResourceType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t *AclsResourceType) UnmarshalText(text []byte) error {
	switch strings.ToUpper(string(text)) {
	case "UNKNOWN", "0":
		*t = AclsResourceTypeUnknown
	case "ANY", "1":
		*t = AclsResourceTypeAny
	case "TOPIC", "2":
		*t = AclsResourceTypeTopic
	case "GROUP", "3":
		*t = AclsResourceTypeGroup
	case "BROKER":
		*t = AclsResourceTypeBroker
	case "CLUSTER":
		*t = AclsResourceTypeCluster
	case "4":
		return fmt.Errorf("4 is ambiguous AclsResourceType, Broker or Cluster?")
	case "TRANSACTIONAL_ID", "5":
		*t = AclsResourceTypeTransactionalID
	case "DELEGATION_TOKEN", "6":
		*t = AclsResourceTypeDelegationToken
	case "USER", "7":
		*t = AclsResourceTypeUser
	default:
		return fmt.Errorf("unknown AclsResourceType: %s", text)
	}
	return nil
}

// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/resource/PatternType.java

type AclsPatternType int8

const (
	AclsPatternTypeUnknown  = 0
	AclsPatternTypeAny      = 1
	AclsPatternTypeMatch    = 2
	AclsPatternTypeLiteral  = 3
	AclsPatternTypePrefixed = 4
)

func (t AclsPatternType) String() string {
	switch t {
	case AclsPatternTypeUnknown:
		return "UNKNOWN"
	case AclsPatternTypeAny:
		return "ANY"
	case AclsPatternTypeMatch:
		return "MATCH"
	case AclsPatternTypeLiteral:
		return "LITERAL"
	case AclsPatternTypePrefixed:
		return "PREFIXED"
	default:
		return "ERROR"
	}
}

func (t AclsPatternType) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t *AclsPatternType) UnmarshalText(text []byte) error {
	switch strings.ToUpper(string(text)) {
	case "UNKNOWN", "0":
		*t = AclsPatternTypeUnknown
	case "ANY", "1":
		*t = AclsPatternTypeAny
	case "MATCH", "2":
		*t = AclsPatternTypeMatch
	case "LITERAL", "3":
		*t = AclsPatternTypeLiteral
	case "PREFIXED", "4":
		*t = AclsPatternTypePrefixed
	default:
		return fmt.Errorf("unknown AclsPatternType: %s", text)
	}
	return nil
}

// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/acl/AclOperation.java
type AclsOperation int8

const AclsOperationUnknown = 0
const AclsOperationAny = 1
const AclsOperationAll = 2
const AclsOperationRead = 3
const AclsOperationWrite = 4
const AclsOperationCreate = 5
const AclsOperationDelete = 6
const AclsOperationAlter = 7
const AclsOperationDescribe = 8
const AclsOperationClusterAction = 9
const AclsOperationDescribeConfigs = 10
const AclsOperationAlterConfigs = 11
const AclsOperationIdempotentWrite = 12

func (o AclsOperation) String() string {
	switch o {
	case AclsOperationUnknown:
		return "UNKNOWN"
	case AclsOperationAny:
		return "ANY"
	case AclsOperationAll:
		return "ALL"
	case AclsOperationRead:
		return "READ"
	case AclsOperationWrite:
		return "WRITE"
	case AclsOperationCreate:
		return "CREATE"
	case AclsOperationDelete:
		return "DELETE"
	case AclsOperationAlter:
		return "ALTER"
	case AclsOperationDescribe:
		return "DESCRIBE"
	case AclsOperationClusterAction:
		return "CLUSTER_ACTION"
	case AclsOperationDescribeConfigs:
		return "DESCRIBE_CONFIGS"
	case AclsOperationAlterConfigs:
		return "ALTER_CONFIGS"
	case AclsOperationIdempotentWrite:
		return "IDEMPOTENT_WRITE"
	default:
		return "ERROR"
	}
}

func (o AclsOperation) MarshalText() ([]byte, error) {
	return []byte(o.String()), nil
}

func (o *AclsOperation) UnmarshalText(text []byte) error {
	switch strings.ToUpper(string(text)) {
	case "UNKNOWN", "0":
		*o = AclsOperationAny
	case "ANY", "1":
		*o = AclsOperationAny
	case "ALL", "2":
		*o = AclsOperationAll
	case "READ", "3":
		*o = AclsOperationRead
	case "WRITE", "4":
		*o = AclsOperationWrite
	case "CREATE", "5":
		*o = AclsOperationCreate
	case "DELETE", "6":
		*o = AclsOperationDelete
	case "ALTER", "7":
		*o = AclsOperationAlter
	case "DESCRIBE", "8":
		*o = AclsOperationDescribe
	case "CLUSTER_ACTION", "9":
		*o = AclsOperationClusterAction
	case "DESCRIBE_CONFIGS", "10":
		*o = AclsOperationDescribeConfigs
	case "ALTER_CONFIGS", "11":
		*o = AclsOperationAlterConfigs
	case "IDEMPOTENT_WRITE", "12":
		*o = AclsOperationIdempotentWrite
	default:
		return fmt.Errorf("unknown AclsOperation: %s", text)
	}
	return nil
}

// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/acl/AclPermissionType.java
type AclsPermissionType int8

const (
	AclsPermissionTypeUnkown AclsPermissionType = 0
	AclsPermissionTypeAny    AclsPermissionType = 1
	AclsPermissionTypeDeny   AclsPermissionType = 2
	AclsPermissionTypeAllow  AclsPermissionType = 3
)

func (p AclsPermissionType) String() string {
	switch p {
	case AclsPermissionTypeUnkown:
		return "UNKNOWN"
	case AclsPermissionTypeAny:
		return "ANY"
	case AclsPermissionTypeDeny:
		return "DENY"
	case AclsPermissionTypeAllow:
		return "ALLOW"
	default:
		return "UNKNOWN"
	}
}

func (p AclsPermissionType) MarshalText() ([]byte, error) {
	return []byte(p.String()), nil
}

func (p *AclsPermissionType) UnmarshalText(text []byte) error {
	switch strings.ToUpper(string(text)) {
	case "UNKNOWN", "0":
		*p = AclsPermissionTypeAny
	case "ANY", "1":
		*p = AclsPermissionTypeAny
	case "DENY", "2":
		*p = AclsPermissionTypeDeny
	case "ALLOW", "3":
		*p = AclsPermissionTypeAllow
	default:
		return fmt.Errorf("unknown AclsPermissionType: %s", text)
	}
	return nil
}
