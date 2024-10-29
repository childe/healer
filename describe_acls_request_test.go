package healer

import (
	"testing"
)

func TestDescribeAclsRequestEncodeAndDecode(t *testing.T) {
	original := DescribeAclsRequest{
		RequestHeader: RequestHeader{
			APIKey:        API_DescribeAcls,
			APIVersion:    0,
			CorrelationID: 10,
		},
		ResourceTypeFilter: 1,
		ResourceNameFilter: "test-resource",
		PrincipalFilter:    "test-principal",
		HostFilter:         "test-host",
		Operation:          2,
		PermissionType:     3,
	}

	var version uint16 = 0
	encoded, err := original.Encode(version)
	if err != nil {
		t.Errorf("encode error: %v", err)
	}

	decoded, err := DecodeDescribeAclsRequest(encoded, version)
	if err != nil {
		t.Errorf("decode error: %v", err)
	}

	if original != decoded {
		t.Errorf("dismatch: %+v, %+v", original, decoded)
	}

}
