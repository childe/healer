package healer

import (
	"testing"
)

func TestDescribeAclsRequestEncodeAndDecodeVersion0(t *testing.T) {
	var version uint16 = 0
	original := DescribeAclsRequest{
		RequestHeader: RequestHeader{
			APIKey:        API_DescribeAcls,
			APIVersion:    version,
			CorrelationID: 10,
		},
		ResourceTypeFilter: 1,
		ResourceNameFilter: "test-resource",
		PrincipalFilter:    "test-principal",
		HostFilter:         "test-host",
		Operation:          2,
		PermissionType:     3,
	}

	encoded := original.Encode(version)

	decoded, err := DecodeDescribeAclsRequest(encoded, version)
	if err != nil {
		t.Errorf("decode error: %v", err)
	}

	if original != decoded {
		t.Errorf("dismatch: %+v, %+v", original, decoded)
	}

}

func TestDescribeAclsRequestEncodeAndDecodeVersion1(t *testing.T) {
	var version uint16 = 1
	original := DescribeAclsRequest{
		RequestHeader: RequestHeader{
			APIKey:        API_DescribeAcls,
			APIVersion:    version,
			CorrelationID: 10,
		},
		ResourceTypeFilter: 1,
		ResourceNameFilter: "test-resource",
		PatternTypeFilter:  3,
		PrincipalFilter:    "test-principal",
		HostFilter:         "test-host",
		Operation:          2,
		PermissionType:     3,
	}

	encoded := original.Encode(version)

	decoded, err := DecodeDescribeAclsRequest(encoded, version)
	if err != nil {
		t.Errorf("decode error: %v", err)
	}

	if original != decoded {
		t.Errorf("dismatch: %+v, %+v", original, decoded)
	}

}
