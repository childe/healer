package healer

import (
	"testing"
)

func TestDescribeAclsResponseEncodeAndDecode(t *testing.T) {
	original := DescribeAclsResponse{
		CorrelationID:  123,
		ThrottleTimeMs: 1000,
		ErrorCode:      0,
		ErrorMessage:   "test error",
		Resources: []AclResource{
			{
				ResourceType: 4,
				ResourceName: "test-topic",
				Acls: []Acl{
					{
						Principal:      "User:test",
						Host:           "*",
						Operation:      2,
						PermissionType: 3,
					},
					{
						Principal:      "User:test2",
						Host:           "1.2.3.4",
						Operation:      4,
						PermissionType: 1,
					},
				},
			},
		},
	}

	encoded, err := original.Encode(0)
	if err != nil {
		t.Fatalf("编码失败: %v", err)
	}

	decoded, err := NewDescribeAclsResponse(encoded, 0)
	if err != nil {
		t.Fatalf("解码失败: %v", err)
	}

	if decoded.CorrelationID != original.CorrelationID {
		t.Errorf("CorrelationID不匹配: 期望 %d, 得到 %d", original.CorrelationID, decoded.CorrelationID)
	}

	if decoded.ThrottleTimeMs != original.ThrottleTimeMs {
		t.Errorf("ThrottleTimeMs不匹配: 期望 %d, 得到 %d", original.ThrottleTimeMs, decoded.ThrottleTimeMs)
	}

	if decoded.ErrorCode != original.ErrorCode {
		t.Errorf("ErrorCode不匹配: 期望 %d, 得到 %d", original.ErrorCode, decoded.ErrorCode)
	}

	if decoded.ErrorMessage != original.ErrorMessage {
		t.Errorf("ErrorMessage不匹配: 期望 %s, 得到 %s", original.ErrorMessage, decoded.ErrorMessage)
	}

	if len(decoded.Resources) != len(original.Resources) {
		t.Fatalf("Resources长度不匹配: 期望 %d, 得到 %d", len(original.Resources), len(decoded.Resources))
	}

	for i, resource := range original.Resources {
		decodedResource := decoded.Resources[i]

		if decodedResource.ResourceType != resource.ResourceType {
			t.Errorf("Resource %d的ResourceType不匹配: 期望 %d, 得到 %d", i, resource.ResourceType, decodedResource.ResourceType)
		}

		if decodedResource.ResourceName != resource.ResourceName {
			t.Errorf("Resource %d的ResourceName不匹配: 期望 %s, 得到 %s", i, resource.ResourceName, decodedResource.ResourceName)
		}

		if len(decodedResource.Acls) != len(resource.Acls) {
			t.Fatalf("Resource %d的Acls长度不匹配: 期望 %d, 得到 %d", i, len(resource.Acls), len(decodedResource.Acls))
		}

		for j, acl := range resource.Acls {
			decodedAcl := decodedResource.Acls[j]

			if decodedAcl.Principal != acl.Principal {
				t.Errorf("Resource %d的Acl %d的Principal不匹配: 期望 %s, 得到 %s", i, j, acl.Principal, decodedAcl.Principal)
			}

			if decodedAcl.Host != acl.Host {
				t.Errorf("Resource %d的Acl %d的Host不匹配: 期望 %s, 得到 %s", i, j, acl.Host, decodedAcl.Host)
			}

			if decodedAcl.Operation != acl.Operation {
				t.Errorf("Resource %d的Acl %d的Operation不匹配: 期望 %d, 得到 %d", i, j, acl.Operation, decodedAcl.Operation)
			}

			if decodedAcl.PermissionType != acl.PermissionType {
				t.Errorf("Resource %d的Acl %d的PermissionType不匹配: 期望 %d, 得到 %d", i, j, acl.PermissionType, decodedAcl.PermissionType)
			}
		}
	}
}
