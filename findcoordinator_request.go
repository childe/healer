package healer

import (
	"encoding/binary"
)

/*
FindCoordinator Request (Version: 0) => group_id
  group_id => STRING

FIELD	DESCRIPTION
group_id	The unique group id.

FindCoordinator Request (Version: 1) => coordinator_key coordinator_type
  coordinator_key => STRING
  coordinator_type => INT8

FIELD	DESCRIPTION
coordinator_key	Id to use for finding the coordinator (for groups, this is the groupId, for transactional producers, this is the transactional id)
coordinator_type	The type of coordinator to find (0 = group, 1 = transaction)
*/

type FindCoordinatorRequest struct {
	RequestHeader *RequestHeader
	GroupID       string
}

func NewFindCoordinatorRequest(correlationID uint32, clientID, groupID string) *FindCoordinatorRequest {
	requestHeader := &RequestHeader{
		ApiKey:        API_FindCoordinator,
		ApiVersion:    0,
		CorrelationID: correlationID,
		ClientId:      clientID,
	}

	return &FindCoordinatorRequest{
		RequestHeader: requestHeader,
		GroupID:       groupID,
	}
}

func (findCoordinatorR *FindCoordinatorRequest) Encode() []byte {
	requestLength := findCoordinatorR.RequestHeader.length() + 2 + len(findCoordinatorR.GroupID)

	payload := make([]byte, requestLength+4)
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], uint32(requestLength))
	offset += 4

	offset = findCoordinatorR.RequestHeader.Encode(payload, offset)

	binary.BigEndian.PutUint16(payload[offset:], uint16(len(findCoordinatorR.GroupID)))
	offset += 2

	copy(payload[offset:], findCoordinatorR.GroupID)

	return payload
}
