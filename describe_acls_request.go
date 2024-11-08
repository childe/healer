package healer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
)

// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/resource/ResourceType.java
type AclsResourceType int8

const (
	DescribeAclsResourceTypeUnknown         = 0
	DescribeAclsResourceTypeAny             = 1
	DescribeAclsResourceTypeTopic           = 2
	DescribeAclsResourceTypeGroup           = 3
	DescribeAclsResourceTypeBroker          = 4
	DescribeAclsResourceTypeCluster         = 4
	DescribeAclsResourceTypeTransactionalID = 5
	DescribeAclsResourceTypeDelegationToken = 6
	DescribeAclsResourceTypeUser            = 7
)

func (t AclsResourceType) String() string {
	switch t {
	case DescribeAclsResourceTypeUnknown:
		return "UNKNOWN"
	case DescribeAclsResourceTypeAny:
		return "ANY"
	case DescribeAclsResourceTypeTopic:
		return "TOPIC"
	case DescribeAclsResourceTypeGroup:
		return "GROUP"
	case DescribeAclsResourceTypeBroker:
		return "BROKER"
	case DescribeAclsResourceTypeTransactionalID:
		return "TRANSACTIONAL_ID"
	case DescribeAclsResourceTypeDelegationToken:
		return "DELEGATION_TOKEN"
	case DescribeAclsResourceTypeUser:
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
		*t = DescribeAclsResourceTypeUnknown
	case "ANY", "1":
		*t = DescribeAclsResourceTypeAny
	case "TOPIC", "2":
		*t = DescribeAclsResourceTypeTopic
	case "GROUP", "3":
		*t = DescribeAclsResourceTypeGroup
	case "BROKER":
		*t = DescribeAclsResourceTypeBroker
	case "CLUSTER":
		*t = DescribeAclsResourceTypeCluster
	case "4":
		return fmt.Errorf("4 is ambiguous DescribeAclsResourceType, Broker or Cluster?")
	case "TRANSACTIONAL_ID", "5":
		*t = DescribeAclsResourceTypeTransactionalID
	case "DELEGATION_TOKEN", "6":
		*t = DescribeAclsResourceTypeDelegationToken
	case "USER", "7":
		*t = DescribeAclsResourceTypeUser
	default:
		return fmt.Errorf("unknown DescribeAclsResourceType: %s", text)
	}
	return nil
}

// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/resource/PatternType.java

type AclsPatternType int8

const (
	DescribeAclsPatternTypeUnknown  = 0
	DescribeAclsPatternTypeAny      = 1
	DescribeAclsPatternTypeMatch    = 2
	DescribeAclsPatternTypeLiteral  = 3
	DescribeAclsPatternTypePrefixed = 4
)

func (t AclsPatternType) String() string {
	switch t {
	case DescribeAclsPatternTypeUnknown:
		return "UNKNOWN"
	case DescribeAclsPatternTypeAny:
		return "ANY"
	case DescribeAclsPatternTypeMatch:
		return "MATCH"
	case DescribeAclsPatternTypeLiteral:
		return "LITERAL"
	case DescribeAclsPatternTypePrefixed:
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
		*t = DescribeAclsPatternTypeUnknown
	case "ANY", "1":
		*t = DescribeAclsPatternTypeAny
	case "MATCH", "2":
		*t = DescribeAclsPatternTypeMatch
	case "LITERAL", "3":
		*t = DescribeAclsPatternTypeLiteral
	case "PREFIXED", "4":
		*t = DescribeAclsPatternTypePrefixed
	default:
		return fmt.Errorf("unknown DescribeAclsPatternType: %s", text)
	}
	return nil
}

// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/acl/AclOperation.java
type AclsOperation int8

const DescribeAclsOperationUnknown = 0
const DescribeAclsOperationAny = 1
const DescribeAclsOperationAll = 2
const DescribeAclsOperationRead = 3
const DescribeAclsOperationWrite = 4
const DescribeAclsOperationCreate = 5
const DescribeAclsOperationDelete = 6
const DescribeAclsOperationAlter = 7
const DescribeAclsOperationDescribe = 8
const DescribeAclsOperationClusterAction = 9
const DescribeAclsOperationDescribeConfigs = 10
const DescribeAclsOperationAlterConfigs = 11
const DescribeAclsOperationIdempotentWrite = 12

func (o AclsOperation) String() string {
	switch o {
	case DescribeAclsOperationUnknown:
		return "UNKNOWN"
	case DescribeAclsOperationAny:
		return "ANY"
	case DescribeAclsOperationAll:
		return "ALL"
	case DescribeAclsOperationRead:
		return "READ"
	case DescribeAclsOperationWrite:
		return "WRITE"
	case DescribeAclsOperationCreate:
		return "CREATE"
	case DescribeAclsOperationDelete:
		return "DELETE"
	case DescribeAclsOperationAlter:
		return "ALTER"
	case DescribeAclsOperationDescribe:
		return "DESCRIBE"
	case DescribeAclsOperationClusterAction:
		return "CLUSTER_ACTION"
	case DescribeAclsOperationDescribeConfigs:
		return "DESCRIBE_CONFIGS"
	case DescribeAclsOperationAlterConfigs:
		return "ALTER_CONFIGS"
	case DescribeAclsOperationIdempotentWrite:
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
		*o = DescribeAclsOperationAny
	case "ANY", "1":
		*o = DescribeAclsOperationAny
	case "ALL", "2":
		*o = DescribeAclsOperationAll
	case "READ", "3":
		*o = DescribeAclsOperationRead
	case "WRITE", "4":
		*o = DescribeAclsOperationWrite
	case "CREATE", "5":
		*o = DescribeAclsOperationCreate
	case "DELETE", "6":
		*o = DescribeAclsOperationDelete
	case "ALTER", "7":
		*o = DescribeAclsOperationAlter
	case "DESCRIBE", "8":
		*o = DescribeAclsOperationDescribe
	case "CLUSTER_ACTION", "9":
		*o = DescribeAclsOperationClusterAction
	case "DESCRIBE_CONFIGS", "10":
		*o = DescribeAclsOperationDescribeConfigs
	case "ALTER_CONFIGS", "11":
		*o = DescribeAclsOperationAlterConfigs
	case "IDEMPOTENT_WRITE", "12":
		*o = DescribeAclsOperationIdempotentWrite
	default:
		return fmt.Errorf("unknown DescribeAclsOperation: %s", text)
	}
	return nil
}

// https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/acl/AclPermissionType.java
type AclsPermissionType int8

const (
	DescribeAclsPermissionTypeUnkown AclsPermissionType = 0
	DescribeAclsPermissionTypeAny    AclsPermissionType = 1
	DescribeAclsPermissionTypeDeny   AclsPermissionType = 2
	DescribeAclsPermissionTypeAllow  AclsPermissionType = 3
)

func (p AclsPermissionType) String() string {
	switch p {
	case DescribeAclsPermissionTypeUnkown:
		return "UNKNOWN"
	case DescribeAclsPermissionTypeAny:
		return "ANY"
	case DescribeAclsPermissionTypeDeny:
		return "DENY"
	case DescribeAclsPermissionTypeAllow:
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
		*p = DescribeAclsPermissionTypeAny
	case "ANY", "1":
		*p = DescribeAclsPermissionTypeAny
	case "DENY", "2":
		*p = DescribeAclsPermissionTypeDeny
	case "ALLOW", "3":
		*p = DescribeAclsPermissionTypeAllow
	default:
		return fmt.Errorf("unknown DescribeAclsPermissionType: %s", text)
	}
	return nil
}

type DescribeAclsRequest struct {
	RequestHeader
	DescribeAclsRequestBody
}
type DescribeAclsRequestBody struct {
	ResourceType   AclsResourceType
	ResourceName   *string
	PatternType    AclsPatternType
	Principal      *string
	Host           *string
	Operation      AclsOperation
	PermissionType AclsPermissionType
	TaggedFields   TaggedFields
}

func (r *DescribeAclsRequest) Encode(version uint16) (rst []byte) {
	buf := new(bytes.Buffer)

	// length
	binary.Write(buf, binary.BigEndian, uint32(0))
	defer func() {
		length := len(rst) - 4
		binary.BigEndian.PutUint32(rst, uint32(length))
	}()

	header := make([]byte, r.RequestHeader.length())
	l := r.RequestHeader.Encode(header)
	buf.Write(header[:l])

	binary.Write(buf, binary.BigEndian, r.ResourceType)

	if version < 2 {
		writeNullableString(buf, r.ResourceName)
	} else {
		writeCompactNullableString(buf, r.ResourceName)
	}

	if version >= 1 {
		binary.Write(buf, binary.BigEndian, r.PatternType)
	}

	if version < 2 {
		writeNullableString(buf, r.Principal)
		writeNullableString(buf, r.Host)
	} else {
		writeCompactNullableString(buf, r.Principal)
		writeCompactNullableString(buf, r.Host)
	}

	binary.Write(buf, binary.BigEndian, r.Operation)

	binary.Write(buf, binary.BigEndian, r.PermissionType)

	buf.Write(r.DescribeAclsRequestBody.TaggedFields.Encode())

	return buf.Bytes()
}

// just for test
func DecodeDescribeAclsRequest(payload []byte, version uint16) (r DescribeAclsRequest, err error) {
	offset := 0

	// request payload length
	binary.BigEndian.Uint32(payload)
	offset += 4

	header, o := DecodeRequestHeader(payload[offset:], version)
	r.RequestHeader = header
	offset += o

	r.ResourceType = AclsResourceType(payload[offset])
	offset++

	if version < 2 {
		resourceName, o := nullableString(payload[offset:])
		offset += o
		r.ResourceName = resourceName
	} else {
		resourceName, o := compactNullableString(payload[offset:])
		offset += o
		r.ResourceName = resourceName
	}

	if version >= 1 {
		r.PatternType = AclsPatternType(payload[offset])
		offset++
	}

	if version < 2 {
		principal, o := nullableString(payload[offset:])
		offset += o
		r.Principal = principal

		host, o := nullableString(payload[offset:])
		offset += o
		r.Host = host
	} else {
		principal, o := compactNullableString(payload[offset:])
		offset += o
		r.Principal = principal

		host, o := compactNullableString(payload[offset:])
		offset += o
		r.Host = host
	}

	r.Operation = AclsOperation(payload[offset])
	offset++

	r.PermissionType = AclsPermissionType(payload[offset])
	offset++

	return r, nil
}
