package healer

import (
	"bytes"
	"encoding/binary"
)

const DescribeAclsTypeGroup = 1
const DescribeAclsTypeTopic = 2

type DescribeAclsRequest struct {
	RequestHeader

	ResourceTypeFilter int8
	ResourceNameFilter string
	PatternTypeFilter  int8
	PrincipalFilter    string
	HostFilter         string
	Operation          int8
	PermissionType     int8
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
	r.ResourceTypeFilter = resourceTypeFilter
	r.ResourceNameFilter = resourceNameFilter
	r.PatternTypeFilter = PatternTypeFilter
	r.PrincipalFilter = principalFilter
	r.HostFilter = hostFilter
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

	binary.Write(buf, binary.BigEndian, r.ResourceTypeFilter)

	writeNullableString(buf, r.ResourceNameFilter)

	if version >= 1 {
		binary.Write(buf, binary.BigEndian, r.PatternTypeFilter)
	}

	writeNullableString(buf, r.PrincipalFilter)

	writeNullableString(buf, r.HostFilter)

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

	r.ResourceTypeFilter = int8(payload[offset])
	offset++

	resourceNameFilter, o := compactNullableString(payload[offset:])
	offset += o
	r.ResourceNameFilter = resourceNameFilter

	if version >= 1 {
		r.PatternTypeFilter = int8(payload[offset])
		offset++
	}

	principalFilter, o := compactNullableString(payload[offset:])
	offset += o
	r.PrincipalFilter = principalFilter

	hostFilter, o := compactNullableString(payload[offset:])
	offset += o
	r.HostFilter = hostFilter

	r.Operation = int8(payload[offset])
	offset++

	r.PermissionType = int8(payload[offset])
	offset++

	return r, nil
}
