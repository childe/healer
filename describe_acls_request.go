package healer

import (
	"bytes"
	"encoding/binary"
)

const DescribeAclsResourceTypeGroup = 1
const DescribeAclsResourceTypeTopic = 2

const DescribeAclsPatternTypeLiteral = 0
const DescribeAclsPatternTypePrefix = 1

const DescribeAclsOperationAny = 0
const DescribeAclsOperationAll = 1
const DescribeAclsOperationRead = 2
const DescribeAclsOperationWrite = 3
const DescribeAclsOperationCreate = 4
const DescribeAclsOperationDelete = 5
const DescribeAclsOperationAlter = 6
const DescribeAclsOperationDescribe = 7
const DescribeAclsOperationClusterAction = 8
const DescribeAclsOperationDescribeConfigs = 9
const DescribeAclsOperationAlterConfigs = 10
const DescribeAclsOperationIdempotentWrite = 11

type DescribeAclsRequest struct {
	RequestHeader

	ResourceType   int8
	ResourceName   string
	PatternType    int8
	Principal      string
	Host           string
	Operation      int8
	PermissionType int8
}

func NewDescribeAclsRequest(
	clientID string,
	resourceTypeFilter int8,
	resourceNameFilter string,
	PatternTypeFilter int8,
	principalFilter string,
	hostFilter string,
	operation int8,
	permissionType int8,
) (r DescribeAclsRequest) {
	r.APIKey = API_DescribeAcls
	r.ClientID = clientID
	r.ResourceType = resourceTypeFilter
	r.ResourceName = resourceNameFilter
	r.PatternType = PatternTypeFilter
	r.Principal = principalFilter
	r.Host = hostFilter
	r.Operation = operation
	r.PermissionType = permissionType
	return
}

func (r *DescribeAclsRequest) Encode(version uint16) (rst []byte) {
	buf := new(bytes.Buffer)

	// length
	binary.Write(buf, binary.BigEndian, uint32(0))
	defer func() {
		length := buf.Len() - 4
		binary.BigEndian.PutUint32(rst, uint32(length))
	}()

	header := make([]byte, r.RequestHeader.length())
	r.RequestHeader.Encode(header)
	buf.Write(header)

	binary.Write(buf, binary.BigEndian, r.ResourceType)

	writeNullableString(buf, r.ResourceName)

	if version >= 1 {
		binary.Write(buf, binary.BigEndian, r.PatternType)
	}

	writeNullableString(buf, r.Principal)

	writeNullableString(buf, r.Host)

	binary.Write(buf, binary.BigEndian, r.Operation)

	binary.Write(buf, binary.BigEndian, r.PermissionType)

	return buf.Bytes()
}

// just for test
func DecodeDescribeAclsRequest(payload []byte, version uint16) (r DescribeAclsRequest, err error) {
	offset := 0

	// request payload length
	binary.BigEndian.Uint32(payload)
	offset += 4

	header, o := DecodeRequestHeader(payload[offset:])
	r.RequestHeader = header
	offset += o

	r.ResourceType = int8(payload[offset])
	offset++

	resourceNameFilter, o := compactNullableString(payload[offset:])
	offset += o
	r.ResourceName = resourceNameFilter

	if version >= 1 {
		r.PatternType = int8(payload[offset])
		offset++
	}

	principalFilter, o := compactNullableString(payload[offset:])
	offset += o
	r.Principal = principalFilter

	hostFilter, o := compactNullableString(payload[offset:])
	offset += o
	r.Host = hostFilter

	r.Operation = int8(payload[offset])
	offset++

	r.PermissionType = int8(payload[offset])
	offset++

	return r, nil
}
