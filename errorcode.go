package healer

import "fmt"

type Error struct {
	Errorcode int
	ErrorMsg  string
	ErrorDesc string
	Retriable bool
}

func (healerError *Error) Error() string {
	return healerError.ErrorDesc
}

var AllError []*Error = make([]*Error, 100)

func getErrorFromErrorCode(errorcode int16) error {
	if errorcode == 0 {
		return nil
	}
	if errorcode == -1 {
		return AllError[0]
	}
	return AllError[errorcode]
}

func init() {
	for i, _ := range AllError {
		AllError[i] = &Error{
			Errorcode: i,
			ErrorMsg:  fmt.Sprintf("NOTDefinedYet%d", i),
			ErrorDesc: fmt.Sprintf("%d not defined yet...", i),
		}
	}

	AllError[0] = &Error{
		Errorcode: -1,
		ErrorMsg:  "UNKNOWN_SERVER_ERROR",
		ErrorDesc: "The server experienced an unexpected error when processing the request",
		Retriable: false,
	}

	AllError[1] = &Error{
		Errorcode: 1,
		ErrorMsg:  "OFFSET_OUT_OF_RANGE",
		ErrorDesc: "The requested offset is not within the range of offsets maintained by the server.",
		Retriable: false,
	}

	AllError[2] = &Error{
		Errorcode: 2,
		ErrorMsg:  "CORRUPT_MESSAGE",
		ErrorDesc: "This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.",
		Retriable: true,
	}

	AllError[3] = &Error{
		Errorcode: 3,
		ErrorMsg:  "UNKNOWN_TOPIC_OR_PARTITION",
		ErrorDesc: "This server does not host this topic-partition.",
		Retriable: true,
	}

	AllError[4] = &Error{
		Errorcode: 4,
		ErrorMsg:  "INVALID_FETCH_SIZE",
		ErrorDesc: "The requested fetch size is invalid.",
		Retriable: false,
	}

	AllError[5] = &Error{
		Errorcode: 5,
		ErrorMsg:  "LEADER_NOT_AVAILABLE",
		ErrorDesc: "There is no leader for this topic-partition as we are in the middle of a leadership election.",
		Retriable: true,
	}

	AllError[6] = &Error{
		Errorcode: 6,
		ErrorMsg:  "NOT_LEADER_FOR_PARTITION",
		ErrorDesc: "This server is not the leader for that topic-partition.",
		Retriable: true,
	}

	AllError[7] = &Error{
		Errorcode: 7,
		ErrorMsg:  "REQUEST_TIMED_OUT",
		ErrorDesc: "The request timed out.",
		Retriable: true,
	}

	AllError[8] = &Error{
		Errorcode: 8,
		ErrorMsg:  "BROKER_NOT_AVAILABLE",
		ErrorDesc: "The broker is not available.",
		Retriable: false,
	}

	AllError[9] = &Error{
		Errorcode: 9,
		ErrorMsg:  "REPLICA_NOT_AVAILABLE",
		ErrorDesc: "The replica is not available for the requested topic-partition",
		Retriable: false,
	}

	AllError[10] = &Error{
		Errorcode: 10,
		ErrorMsg:  "MESSAGE_TOO_LARGE",
		ErrorDesc: "The request included a message larger than the max message size the server will accept.",
		Retriable: false,
	}

	AllError[11] = &Error{
		Errorcode: 11,
		ErrorMsg:  "STALE_CONTROLLER_EPOCH",
		ErrorDesc: "The controller moved to another broker.",
		Retriable: false,
	}

	AllError[12] = &Error{
		Errorcode: 12,
		ErrorMsg:  "OFFSET_METADATA_TOO_LARGE",
		ErrorDesc: "The metadata field of the offset request was too large.",
		Retriable: false,
	}

	AllError[13] = &Error{
		Errorcode: 13,
		ErrorMsg:  "NETWORK_EXCEPTION",
		ErrorDesc: "The server disconnected before a response was received.",
		Retriable: true,
	}

	AllError[14] = &Error{
		Errorcode: 14,
		ErrorMsg:  "COORDINATOR_LOAD_IN_PROGRESS",
		ErrorDesc: "The coordinator is loading and hence can't process requests.",
		Retriable: true,
	}

	AllError[15] = &Error{
		Errorcode: 15,
		ErrorMsg:  "COORDINATOR_NOT_AVAILABLE",
		ErrorDesc: "The coordinator is not available.",
		Retriable: true,
	}

	AllError[16] = &Error{
		Errorcode: 16,
		ErrorMsg:  "NOT_COORDINATOR",
		ErrorDesc: "This is not the correct coordinator.",
		Retriable: true,
	}

	AllError[17] = &Error{
		Errorcode: 17,
		ErrorMsg:  "INVALID_TOPIC_EXCEPTION",
		ErrorDesc: "The request attempted to perform an operation on an invalid topic.",
		Retriable: false,
	}

	AllError[18] = &Error{
		Errorcode: 18,
		ErrorMsg:  "RECORD_LIST_TOO_LARGE",
		ErrorDesc: "The request included message batch larger than the configured segment size on the server.",
		Retriable: false,
	}

	AllError[19] = &Error{
		Errorcode: 19,
		ErrorMsg:  "NOT_ENOUGH_REPLICAS",
		ErrorDesc: "Messages are rejected since there are fewer in-sync replicas than required.",
		Retriable: true,
	}

	AllError[20] = &Error{
		Errorcode: 20,
		ErrorMsg:  "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
		ErrorDesc: "Messages are written to the log, but to fewer in-sync replicas than required.",
		Retriable: true,
	}

	AllError[21] = &Error{
		Errorcode: 21,
		ErrorMsg:  "INVALID_REQUIRED_ACKS",
		ErrorDesc: "Produce request specified an invalid value for required acks.",
		Retriable: false,
	}

	AllError[22] = &Error{
		Errorcode: 22,
		ErrorMsg:  "ILLEGAL_GENERATION",
		ErrorDesc: "Specified group generation id is not valid.",
		Retriable: false,
	}

	AllError[23] = &Error{
		Errorcode: 23,
		ErrorMsg:  "INCONSISTENT_GROUP_PROTOCOL",
		ErrorDesc: "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.",
		Retriable: false,
	}

	AllError[24] = &Error{
		Errorcode: 24,
		ErrorMsg:  "INVALID_GROUP_ID",
		ErrorDesc: "The configured groupId is invalid",
		Retriable: false,
	}

	AllError[25] = &Error{
		Errorcode: 25,
		ErrorMsg:  "UNKNOWN_MEMBER_ID",
		ErrorDesc: "The coordinator is not aware of this member.",
		Retriable: false,
	}

	AllError[26] = &Error{
		Errorcode: 26,
		ErrorMsg:  "INVALID_SESSION_TIMEOUT",
		ErrorDesc: "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).",
		Retriable: false,
	}

	AllError[27] = &Error{
		Errorcode: 27,
		ErrorMsg:  "REBALANCE_IN_PROGRESS",
		ErrorDesc: "The group is rebalancing, so a rejoin is needed.",
		Retriable: false,
	}

	AllError[28] = &Error{
		Errorcode: 28,
		ErrorMsg:  "INVALID_COMMIT_OFFSET_SIZE",
		ErrorDesc: "The committing offset data size is not valid",
		Retriable: false,
	}

	AllError[29] = &Error{
		Errorcode: 29,
		ErrorMsg:  "TOPIC_AUTHORIZATION_FAILED",
		ErrorDesc: "Not authorized to access topics: [Topic authorization failed.]",
		Retriable: false,
	}

	AllError[30] = &Error{
		Errorcode: 30,
		ErrorMsg:  "GROUP_AUTHORIZATION_FAILED",
		ErrorDesc: "Not authorized to access group: Group authorization failed.",
		Retriable: false,
	}

	AllError[31] = &Error{
		Errorcode: 31,
		ErrorMsg:  "CLUSTER_AUTHORIZATION_FAILED",
		ErrorDesc: "Cluster authorization failed.",
		Retriable: false,
	}

	AllError[32] = &Error{
		Errorcode: 32,
		ErrorMsg:  "INVALID_TIMESTAMP",
		ErrorDesc: "The timestamp of the message is out of acceptable range.",
		Retriable: false,
	}

	AllError[33] = &Error{
		Errorcode: 33,
		ErrorMsg:  "UNSUPPORTED_SASL_MECHANISM",
		ErrorDesc: "The broker does not support the requested SASL mechanism.",
		Retriable: false,
	}

	AllError[34] = &Error{
		Errorcode: 34,
		ErrorMsg:  "ILLEGAL_SASL_STATE",
		ErrorDesc: "Request is not valid given the current SASL state.",
		Retriable: false,
	}

	AllError[35] = &Error{
		Errorcode: 35,
		ErrorMsg:  "UNSUPPORTED_VERSION",
		ErrorDesc: "The version of API is not supported.",
		Retriable: false,
	}

	AllError[36] = &Error{
		Errorcode: 36,
		ErrorMsg:  "TOPIC_ALREADY_EXISTS",
		ErrorDesc: "Topic with this name already exists.",
		Retriable: false,
	}

	AllError[37] = &Error{
		Errorcode: 37,
		ErrorMsg:  "INVALID_PARTITIONS",
		ErrorDesc: "Number of partitions is invalid.",
		Retriable: false,
	}

	AllError[38] = &Error{
		Errorcode: 38,
		ErrorMsg:  "INVALID_REPLICATION_FACTOR",
		ErrorDesc: "Replication-factor is invalid.",
		Retriable: false,
	}

	AllError[39] = &Error{
		Errorcode: 39,
		ErrorMsg:  "INVALID_REPLICA_ASSIGNMENT",
		ErrorDesc: "Replica assignment is invalid.",
		Retriable: false,
	}

	AllError[40] = &Error{
		Errorcode: 40,
		ErrorMsg:  "INVALID_CONFIG",
		ErrorDesc: "Configuration is invalid.",
		Retriable: false,
	}

	AllError[41] = &Error{
		Errorcode: 41,
		ErrorMsg:  "NOT_CONTROLLER",
		ErrorDesc: "This is not the correct controller for this cluster.",
		Retriable: true,
	}

	AllError[42] = &Error{
		Errorcode: 42,
		ErrorMsg:  "INVALID_REQUEST",
		ErrorDesc: "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.",
		Retriable: false,
	}

	AllError[43] = &Error{
		Errorcode: 43,
		ErrorMsg:  "UNSUPPORTED_FOR_MESSAGE_FORMAT",
		ErrorDesc: "The message format version on the broker does not support the request.",
		Retriable: false,
	}

	AllError[44] = &Error{
		Errorcode: 44,
		ErrorMsg:  "POLICY_VIOLATION",
		ErrorDesc: "Request parameters do not satisfy the configured policy.",
		Retriable: false,
	}

	AllError[45] = &Error{
		Errorcode: 45,
		ErrorMsg:  "OUT_OF_ORDER_SEQUENCE_NUMBER",
		ErrorDesc: "The broker received an out of order sequence number",
		Retriable: false,
	}

	AllError[46] = &Error{
		Errorcode: 46,
		ErrorMsg:  "DUPLICATE_SEQUENCE_NUMBER",
		ErrorDesc: "The broker received a duplicate sequence number",
		Retriable: false,
	}

	AllError[47] = &Error{
		Errorcode: 47,
		ErrorMsg:  "INVALID_PRODUCER_EPOCH",
		ErrorDesc: "Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker.",
		Retriable: false,
	}

	AllError[48] = &Error{
		Errorcode: 48,
		ErrorMsg:  "INVALID_TXN_STATE",
		ErrorDesc: "The producer attempted a transactional operation in an invalid state",
		Retriable: false,
	}

	AllError[49] = &Error{
		Errorcode: 49,
		ErrorMsg:  "INVALID_PRODUCER_ID_MAPPING",
		ErrorDesc: "The producer attempted to use a producer id which is not currently assigned to its transactional id",
		Retriable: false,
	}

	AllError[50] = &Error{
		Errorcode: 50,
		ErrorMsg:  "INVALID_TRANSACTION_TIMEOUT",
		ErrorDesc: "The transaction timeout is larger than the maximum value allowed by the broker (as configured by max.transaction.timeout.ms).",
		Retriable: false,
	}

	AllError[51] = &Error{
		Errorcode: 51,
		ErrorMsg:  "CONCURRENT_TRANSACTIONS",
		ErrorDesc: "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing",
		Retriable: false,
	}

	AllError[52] = &Error{
		Errorcode: 52,
		ErrorMsg:  "TRANSACTION_COORDINATOR_FENCED",
		ErrorDesc: "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer",
		Retriable: false,
	}

	AllError[53] = &Error{
		Errorcode: 53,
		ErrorMsg:  "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
		ErrorDesc: "Transactional Id authorization failed",
		Retriable: false,
	}

	AllError[54] = &Error{
		Errorcode: 54,
		ErrorMsg:  "SECURITY_DISABLED",
		ErrorDesc: "Security features are disabled.",
		Retriable: false,
	}

	AllError[55] = &Error{
		Errorcode: 55,
		ErrorMsg:  "OPERATION_NOT_ATTEMPTED",
		ErrorDesc: "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.",
		Retriable: false,
	}

	AllError[56] = &Error{
		Errorcode: 56,
		ErrorMsg:  "KAFKA_STORAGE_ERROR",
		ErrorDesc: "Disk error when trying to access log file on the disk.",
		Retriable: true,
	}

	AllError[57] = &Error{
		Errorcode: 57,
		ErrorMsg:  "LOG_DIR_NOT_FOUND",
		ErrorDesc: "The user-specified log directory is not found in the broker config.",
		Retriable: false,
	}

	AllError[58] = &Error{
		Errorcode: 58,
		ErrorMsg:  "SASL_AUTHENTICATION_FAILED",
		ErrorDesc: "SASL Authentication failed.",
		Retriable: false,
	}

	AllError[59] = &Error{
		Errorcode: 59,
		ErrorMsg:  "UNKNOWN_PRODUCER_ID",
		ErrorDesc: "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.",
		Retriable: false,
	}

	AllError[60] = &Error{
		Errorcode: 60,
		ErrorMsg:  "REASSIGNMENT_IN_PROGRESS",
		ErrorDesc: "A partition reassignment is in progress",
		Retriable: false,
	}
	AllError[61] = &Error{
		ErrorMsg:  "DELEGATION_TOKEN_AUTH_DISABLED",
		Errorcode: 61,
		ErrorDesc: "Delegation Token feature is not enabled.",
		Retriable: false,
	}
	AllError[62] = &Error{
		ErrorMsg:  "DELEGATION_TOKEN_NOT_FOUND",
		Errorcode: 62,
		ErrorDesc: "Delegation Token is not found on server.",
		Retriable: false,
	}
	AllError[63] = &Error{
		ErrorMsg:  "DELEGATION_TOKEN_OWNER_MISMATCH",
		Errorcode: 63,
		ErrorDesc: "Specified Principal is not valid Owner/Renewer.",
		Retriable: false,
	}
	AllError[64] = &Error{
		ErrorMsg:  "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
		Errorcode: 64,
		ErrorDesc: "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.",
		Retriable: false,
	}
	AllError[65] = &Error{
		ErrorMsg:  "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
		Errorcode: 65,
		ErrorDesc: "Delegation Token authorization failed",
		Retriable: false,
	}
	AllError[66] = &Error{
		ErrorMsg:  "DELEGATION_TOKEN_EXPIRED",
		Errorcode: 66,
		ErrorDesc: "Delegation Token is expired.",
		Retriable: false,
	}
	AllError[67] = &Error{
		ErrorMsg:  "INVALID_PRINCIPAL_TYPE",
		Errorcode: 67,
		ErrorDesc: "Supplied principalType is not supported.",
		Retriable: false,
	}
	AllError[68] = &Error{
		ErrorMsg:  "NON_EMPTY_GROUP",
		Errorcode: 68,
		ErrorDesc: "The group is not empty.",
		Retriable: false,
	}
	AllError[69] = &Error{
		ErrorMsg:  "GROUP_ID_NOT_FOUND",
		Errorcode: 69,
		ErrorDesc: "The group id does not exist.",
		Retriable: false,
	}
	AllError[70] = &Error{
		ErrorMsg:  "FETCH_SESSION_ID_NOT_FOUND",
		Errorcode: 70,
		ErrorDesc: "The fetch session ID was not found.",
		Retriable: true,
	}
	AllError[71] = &Error{
		ErrorMsg:  "INVALID_FETCH_SESSION_EPOCH",
		Errorcode: 71,
		ErrorDesc: "The fetch session epoch is invalid.",
		Retriable: true,
	}
	AllError[72] = &Error{
		ErrorMsg:  "LISTENER_NOT_FOUND",
		Errorcode: 72,
		ErrorDesc: "There is no listener on the leader broker that matches the listener on which metadata request was processed.",
		Retriable: true,
	}
	AllError[73] = &Error{
		ErrorMsg:  "TOPIC_DELETION_DISABLED",
		Errorcode: 73,
		ErrorDesc: "Topic deletion is disabled.",
		Retriable: false,
	}
	AllError[74] = &Error{
		ErrorMsg:  "FENCED_LEADER_EPOCH",
		Errorcode: 74,
		ErrorDesc: "The leader epoch in the request is older than the epoch on the broker",
		Retriable: true,
	}
	AllError[75] = &Error{
		ErrorMsg:  "UNKNOWN_LEADER_EPOCH",
		Errorcode: 75,
		ErrorDesc: "The leader epoch in the request is newer than the epoch on the broker",
		Retriable: true,
	}
	AllError[76] = &Error{
		ErrorMsg:  "UNSUPPORTED_COMPRESSION_TYPE",
		Errorcode: 76,
		ErrorDesc: "The requesting client does not support the compression type of given partition.",
		Retriable: false,
	}
}
