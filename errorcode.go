package healer

import "fmt"

var AllError map[int16]*Error = make(map[int16]*Error)

type KafkaError int16

func (kafkaError KafkaError) Error() string {
	if int16(kafkaError) == 0 {
		return "no error"
	}
	if e, ok := AllError[int16(kafkaError)]; ok {
		return fmt.Sprintf("%s:%s", e.ErrorMsg, e.ErrorDesc)
	}
	return fmt.Sprintf("unknown error code:%d", int16(kafkaError))
}

func (kafkaError KafkaError) IsRetriable() bool {
	if e, ok := AllError[int16(kafkaError)]; ok {
		return e.Retriable
	}
	return false
}

type Error struct {
	ErrorCode int16
	ErrorMsg  string
	ErrorDesc string
	Retriable bool
}

func getErrorFromErrorCode(errorcode int16) error {
	if errorcode == 0 {
		return nil
	}

	if _, ok := AllError[errorcode]; ok {
		return KafkaError(errorcode)
	}

	return fmt.Errorf("unknown error code:%d", errorcode)
}

func init() {
	AllError[-1] = &Error{
		ErrorCode: -1,
		ErrorMsg:  "UNKNOWN_SERVER_ERROR",
		ErrorDesc: "The server experienced an unexpected error when processing the request",
		Retriable: false,
	}

	AllError[1] = &Error{
		ErrorMsg:  "OFFSET_OUT_OF_RANGE",
		ErrorCode: 1,
		Retriable: false,
		ErrorDesc: "The requested offset is not within the range of offsets maintained by the server.",
	}
	AllError[2] = &Error{
		ErrorMsg:  "CORRUPT_MESSAGE",
		ErrorCode: 2,
		Retriable: true,
		ErrorDesc: "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.",
	}
	AllError[3] = &Error{
		ErrorMsg:  "UNKNOWN_TOPIC_OR_PARTITION",
		ErrorCode: 3,
		Retriable: true,
		ErrorDesc: "This server does not host this topic-partition.",
	}
	AllError[4] = &Error{
		ErrorMsg:  "INVALID_FETCH_SIZE",
		ErrorCode: 4,
		Retriable: false,
		ErrorDesc: "The requested fetch size is invalid.",
	}
	AllError[5] = &Error{
		ErrorMsg:  "LEADER_NOT_AVAILABLE",
		ErrorCode: 5,
		Retriable: true,
		ErrorDesc: "There is no leader for this topic-partition as we are in the middle of a leadership election.",
	}
	AllError[6] = &Error{
		ErrorMsg:  "NOT_LEADER_OR_FOLLOWER",
		ErrorCode: 6,
		Retriable: true,
		ErrorDesc: "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.",
	}
	AllError[7] = &Error{
		ErrorMsg:  "REQUEST_TIMED_OUT",
		ErrorCode: 7,
		Retriable: true,
		ErrorDesc: "The request timed out.",
	}
	AllError[8] = &Error{
		ErrorMsg:  "BROKER_NOT_AVAILABLE",
		ErrorCode: 8,
		Retriable: false,
		ErrorDesc: "The broker is not available.",
	}
	AllError[9] = &Error{
		ErrorMsg:  "REPLICA_NOT_AVAILABLE",
		ErrorCode: 9,
		Retriable: true,
		ErrorDesc: "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.",
	}
	AllError[10] = &Error{
		ErrorMsg:  "MESSAGE_TOO_LARGE",
		ErrorCode: 10,
		Retriable: false,
		ErrorDesc: "The request included a message larger than the max message size the server will accept.",
	}
	AllError[11] = &Error{
		ErrorMsg:  "STALE_CONTROLLER_EPOCH",
		ErrorCode: 11,
		Retriable: false,
		ErrorDesc: "The controller moved to another broker.",
	}
	AllError[12] = &Error{
		ErrorMsg:  "OFFSET_METADATA_TOO_LARGE",
		ErrorCode: 12,
		Retriable: false,
		ErrorDesc: "The metadata field of the offset request was too large.",
	}
	AllError[13] = &Error{
		ErrorMsg:  "NETWORK_EXCEPTION",
		ErrorCode: 13,
		Retriable: true,
		ErrorDesc: "The server disconnected before a response was received.",
	}
	AllError[14] = &Error{
		ErrorMsg:  "COORDINATOR_LOAD_IN_PROGRESS",
		ErrorCode: 14,
		Retriable: true,
		ErrorDesc: "The coordinator is loading and hence can't process requests.",
	}
	AllError[15] = &Error{
		ErrorMsg:  "COORDINATOR_NOT_AVAILABLE",
		ErrorCode: 15,
		Retriable: true,
		ErrorDesc: "The coordinator is not available.",
	}
	AllError[16] = &Error{
		ErrorMsg:  "NOT_COORDINATOR",
		ErrorCode: 16,
		Retriable: true,
		ErrorDesc: "This is not the correct coordinator.",
	}
	AllError[17] = &Error{
		ErrorMsg:  "INVALID_TOPIC_EXCEPTION",
		ErrorCode: 17,
		Retriable: false,
		ErrorDesc: "The request attempted to perform an operation on an invalid topic.",
	}
	AllError[18] = &Error{
		ErrorMsg:  "RECORD_LIST_TOO_LARGE",
		ErrorCode: 18,
		Retriable: false,
		ErrorDesc: "The request included message batch larger than the configured segment size on the server.",
	}
	AllError[19] = &Error{
		ErrorMsg:  "NOT_ENOUGH_REPLICAS",
		ErrorCode: 19,
		Retriable: true,
		ErrorDesc: "Messages are rejected since there are fewer in-sync replicas than required.",
	}
	AllError[20] = &Error{
		ErrorMsg:  "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
		ErrorCode: 20,
		Retriable: true,
		ErrorDesc: "Messages are written to the log, but to fewer in-sync replicas than required.",
	}
	AllError[21] = &Error{
		ErrorMsg:  "INVALID_REQUIRED_ACKS",
		ErrorCode: 21,
		Retriable: false,
		ErrorDesc: "Produce request specified an invalid value for required acks.",
	}
	AllError[22] = &Error{
		ErrorMsg:  "ILLEGAL_GENERATION",
		ErrorCode: 22,
		Retriable: false,
		ErrorDesc: "Specified group generation id is not valid.",
	}
	AllError[23] = &Error{
		ErrorMsg:  "INCONSISTENT_GROUP_PROTOCOL",
		ErrorCode: 23,
		Retriable: false,
		ErrorDesc: "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.",
	}
	AllError[24] = &Error{
		ErrorMsg:  "INVALID_GROUP_ID",
		ErrorCode: 24,
		Retriable: false,
		ErrorDesc: "The configured groupId is invalid.",
	}
	AllError[25] = &Error{
		ErrorMsg:  "UNKNOWN_MEMBER_ID",
		ErrorCode: 25,
		Retriable: false,
		ErrorDesc: "The coordinator is not aware of this member.",
	}
	AllError[26] = &Error{
		ErrorMsg:  "INVALID_SESSION_TIMEOUT",
		ErrorCode: 26,
		Retriable: false,
		ErrorDesc: "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).",
	}
	AllError[27] = &Error{
		ErrorMsg:  "REBALANCE_IN_PROGRESS",
		ErrorCode: 27,
		Retriable: false,
		ErrorDesc: "The group is rebalancing, so a rejoin is needed.",
	}
	AllError[28] = &Error{
		ErrorMsg:  "INVALID_COMMIT_OFFSET_SIZE",
		ErrorCode: 28,
		Retriable: false,
		ErrorDesc: "The committing offset data size is not valid.",
	}
	AllError[29] = &Error{
		ErrorMsg:  "TOPIC_AUTHORIZATION_FAILED",
		ErrorCode: 29,
		Retriable: false,
		ErrorDesc: "Topic authorization failed.",
	}
	AllError[30] = &Error{
		ErrorMsg:  "GROUP_AUTHORIZATION_FAILED",
		ErrorCode: 30,
		Retriable: false,
		ErrorDesc: "Group authorization failed.",
	}
	AllError[31] = &Error{
		ErrorMsg:  "CLUSTER_AUTHORIZATION_FAILED",
		ErrorCode: 31,
		Retriable: false,
		ErrorDesc: "Cluster authorization failed.",
	}
	AllError[32] = &Error{
		ErrorMsg:  "INVALID_TIMESTAMP",
		ErrorCode: 32,
		Retriable: false,
		ErrorDesc: "The timestamp of the message is out of acceptable range.",
	}
	AllError[33] = &Error{
		ErrorMsg:  "UNSUPPORTED_SASL_MECHANISM",
		ErrorCode: 33,
		Retriable: false,
		ErrorDesc: "The broker does not support the requested SASL mechanism.",
	}
	AllError[34] = &Error{
		ErrorMsg:  "ILLEGAL_SASL_STATE",
		ErrorCode: 34,
		Retriable: false,
		ErrorDesc: "Request is not valid given the current SASL state.",
	}
	AllError[35] = &Error{
		ErrorMsg:  "UNSUPPORTED_VERSION",
		ErrorCode: 35,
		Retriable: false,
		ErrorDesc: "The version of API is not supported.",
	}
	AllError[36] = &Error{
		ErrorMsg:  "TOPIC_ALREADY_EXISTS",
		ErrorCode: 36,
		Retriable: false,
		ErrorDesc: "Topic with this name already exists.",
	}
	AllError[37] = &Error{
		ErrorMsg:  "INVALID_PARTITIONS",
		ErrorCode: 37,
		Retriable: false,
		ErrorDesc: "Number of partitions is below 1.",
	}
	AllError[38] = &Error{
		ErrorMsg:  "INVALID_REPLICATION_FACTOR",
		ErrorCode: 38,
		Retriable: false,
		ErrorDesc: "Replication factor is below 1 or larger than the number of available brokers.",
	}
	AllError[39] = &Error{
		ErrorMsg:  "INVALID_REPLICA_ASSIGNMENT",
		ErrorCode: 39,
		Retriable: false,
		ErrorDesc: "Replica assignment is invalid.",
	}
	AllError[40] = &Error{
		ErrorMsg:  "INVALID_CONFIG",
		ErrorCode: 40,
		Retriable: false,
		ErrorDesc: "Configuration is invalid.",
	}
	AllError[41] = &Error{
		ErrorMsg:  "NOT_CONTROLLER",
		ErrorCode: 41,
		Retriable: true,
		ErrorDesc: "This is not the correct controller for this cluster.",
	}
	AllError[42] = &Error{
		ErrorMsg:  "INVALID_REQUEST",
		ErrorCode: 42,
		Retriable: false,
		ErrorDesc: "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.",
	}
	AllError[43] = &Error{
		ErrorMsg:  "UNSUPPORTED_FOR_MESSAGE_FORMAT",
		ErrorCode: 43,
		Retriable: false,
		ErrorDesc: "The message format version on the broker does not support the request.",
	}
	AllError[44] = &Error{
		ErrorMsg:  "POLICY_VIOLATION",
		ErrorCode: 44,
		Retriable: false,
		ErrorDesc: "Request parameters do not satisfy the configured policy.",
	}
	AllError[45] = &Error{
		ErrorMsg:  "OUT_OF_ORDER_SEQUENCE_NUMBER",
		ErrorCode: 45,
		Retriable: false,
		ErrorDesc: "The broker received an out of order sequence number.",
	}
	AllError[46] = &Error{
		ErrorMsg:  "DUPLICATE_SEQUENCE_NUMBER",
		ErrorCode: 46,
		Retriable: false,
		ErrorDesc: "The broker received a duplicate sequence number.",
	}
	AllError[47] = &Error{
		ErrorMsg:  "INVALID_PRODUCER_EPOCH",
		ErrorCode: 47,
		Retriable: false,
		ErrorDesc: "Producer attempted to produce with an old epoch.",
	}
	AllError[48] = &Error{
		ErrorMsg:  "INVALID_TXN_STATE",
		ErrorCode: 48,
		Retriable: false,
		ErrorDesc: "The producer attempted a transactional operation in an invalid state.",
	}
	AllError[49] = &Error{
		ErrorMsg:  "INVALID_PRODUCER_ID_MAPPING",
		ErrorCode: 49,
		Retriable: false,
		ErrorDesc: "The producer attempted to use a producer id which is not currently assigned to its transactional id.",
	}
	AllError[50] = &Error{
		ErrorMsg:  "INVALID_TRANSACTION_TIMEOUT",
		ErrorCode: 50,
		Retriable: false,
		ErrorDesc: "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).",
	}
	AllError[51] = &Error{
		ErrorMsg:  "CONCURRENT_TRANSACTIONS",
		ErrorCode: 51,
		Retriable: true,
		ErrorDesc: "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.",
	}
	AllError[52] = &Error{
		ErrorMsg:  "TRANSACTION_COORDINATOR_FENCED",
		ErrorCode: 52,
		Retriable: false,
		ErrorDesc: "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.",
	}
	AllError[53] = &Error{
		ErrorMsg:  "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
		ErrorCode: 53,
		Retriable: false,
		ErrorDesc: "Transactional Id authorization failed.",
	}
	AllError[54] = &Error{
		ErrorMsg:  "SECURITY_DISABLED",
		ErrorCode: 54,
		Retriable: false,
		ErrorDesc: "Security features are disabled.",
	}
	AllError[55] = &Error{
		ErrorMsg:  "OPERATION_NOT_ATTEMPTED",
		ErrorCode: 55,
		Retriable: false,
		ErrorDesc: "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.",
	}
	AllError[56] = &Error{
		ErrorMsg:  "KAFKA_STORAGE_ERROR",
		ErrorCode: 56,
		Retriable: true,
		ErrorDesc: "Disk error when trying to access log file on the disk.",
	}
	AllError[57] = &Error{
		ErrorMsg:  "LOG_DIR_NOT_FOUND",
		ErrorCode: 57,
		Retriable: false,
		ErrorDesc: "The user-specified log directory is not found in the broker config.",
	}
	AllError[58] = &Error{
		ErrorMsg:  "SASL_AUTHENTICATION_FAILED",
		ErrorCode: 58,
		Retriable: false,
		ErrorDesc: "SASL Authentication failed.",
	}
	AllError[59] = &Error{
		ErrorMsg:  "UNKNOWN_PRODUCER_ID",
		ErrorCode: 59,
		Retriable: false,
		ErrorDesc: "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.",
	}
	AllError[60] = &Error{
		ErrorMsg:  "REASSIGNMENT_IN_PROGRESS",
		ErrorCode: 60,
		Retriable: false,
		ErrorDesc: "A partition reassignment is in progress.",
	}
	AllError[61] = &Error{
		ErrorMsg:  "DELEGATION_TOKEN_AUTH_DISABLED",
		ErrorCode: 61,
		Retriable: false,
		ErrorDesc: "Delegation Token feature is not enabled.",
	}
	AllError[62] = &Error{
		ErrorMsg:  "DELEGATION_TOKEN_NOT_FOUND",
		ErrorCode: 62,
		Retriable: false,
		ErrorDesc: "Delegation Token is not found on server.",
	}
	AllError[63] = &Error{
		ErrorMsg:  "DELEGATION_TOKEN_OWNER_MISMATCH",
		ErrorCode: 63,
		Retriable: false,
		ErrorDesc: "Specified Principal is not valid Owner/Renewer.",
	}
	AllError[64] = &Error{
		ErrorMsg:  "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
		ErrorCode: 64,
		Retriable: false,
		ErrorDesc: "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.",
	}
	AllError[65] = &Error{
		ErrorMsg:  "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
		ErrorCode: 65,
		Retriable: false,
		ErrorDesc: "Delegation Token authorization failed.",
	}
	AllError[66] = &Error{
		ErrorMsg:  "DELEGATION_TOKEN_EXPIRED",
		ErrorCode: 66,
		Retriable: false,
		ErrorDesc: "Delegation Token is expired.",
	}
	AllError[67] = &Error{
		ErrorMsg:  "INVALID_PRINCIPAL_TYPE",
		ErrorCode: 67,
		Retriable: false,
		ErrorDesc: "Supplied principalType is not supported.",
	}
	AllError[68] = &Error{
		ErrorMsg:  "NON_EMPTY_GROUP",
		ErrorCode: 68,
		Retriable: false,
		ErrorDesc: "The group is not empty.",
	}
	AllError[69] = &Error{
		ErrorMsg:  "GROUP_ID_NOT_FOUND",
		ErrorCode: 69,
		Retriable: false,
		ErrorDesc: "The group id does not exist.",
	}
	AllError[70] = &Error{
		ErrorMsg:  "FETCH_SESSION_ID_NOT_FOUND",
		ErrorCode: 70,
		Retriable: true,
		ErrorDesc: "The fetch session ID was not found.",
	}
	AllError[71] = &Error{
		ErrorMsg:  "INVALID_FETCH_SESSION_EPOCH",
		ErrorCode: 71,
		Retriable: true,
		ErrorDesc: "The fetch session epoch is invalid.",
	}
	AllError[72] = &Error{
		ErrorMsg:  "LISTENER_NOT_FOUND",
		ErrorCode: 72,
		Retriable: true,
		ErrorDesc: "There is no listener on the leader broker that matches the listener on which metadata request was processed.",
	}
	AllError[73] = &Error{
		ErrorMsg:  "TOPIC_DELETION_DISABLED",
		ErrorCode: 73,
		Retriable: false,
		ErrorDesc: "Topic deletion is disabled.",
	}
	AllError[74] = &Error{
		ErrorMsg:  "FENCED_LEADER_EPOCH",
		ErrorCode: 74,
		Retriable: true,
		ErrorDesc: "The leader epoch in the request is older than the epoch on the broker.",
	}
	AllError[75] = &Error{
		ErrorMsg:  "UNKNOWN_LEADER_EPOCH",
		ErrorCode: 75,
		Retriable: true,
		ErrorDesc: "The leader epoch in the request is newer than the epoch on the broker.",
	}
	AllError[76] = &Error{
		ErrorMsg:  "UNSUPPORTED_COMPRESSION_TYPE",
		ErrorCode: 76,
		Retriable: false,
		ErrorDesc: "The requesting client does not support the compression type of given partition.",
	}
	AllError[77] = &Error{
		ErrorMsg:  "STALE_BROKER_EPOCH",
		ErrorCode: 77,
		Retriable: false,
		ErrorDesc: "Broker epoch has changed.",
	}
	AllError[78] = &Error{
		ErrorMsg:  "OFFSET_NOT_AVAILABLE",
		ErrorCode: 78,
		Retriable: true,
		ErrorDesc: "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.",
	}
	AllError[79] = &Error{
		ErrorMsg:  "MEMBER_ID_REQUIRED",
		ErrorCode: 79,
		Retriable: false,
		ErrorDesc: "The group member needs to have a valid member id before actually entering a consumer group.",
	}
	AllError[80] = &Error{
		ErrorMsg:  "PREFERRED_LEADER_NOT_AVAILABLE",
		ErrorCode: 80,
		Retriable: true,
		ErrorDesc: "The preferred leader was not available.",
	}
	AllError[81] = &Error{
		ErrorMsg:  "GROUP_MAX_SIZE_REACHED",
		ErrorCode: 81,
		Retriable: false,
		ErrorDesc: "The consumer group has reached its max size.",
	}
	AllError[82] = &Error{
		ErrorMsg:  "FENCED_INSTANCE_ID",
		ErrorCode: 82,
		Retriable: false,
		ErrorDesc: "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.",
	}
	AllError[83] = &Error{
		ErrorMsg:  "ELIGIBLE_LEADERS_NOT_AVAILABLE",
		ErrorCode: 83,
		Retriable: true,
		ErrorDesc: "Eligible topic partition leaders are not available.",
	}
	AllError[84] = &Error{
		ErrorMsg:  "ELECTION_NOT_NEEDED",
		ErrorCode: 84,
		Retriable: true,
		ErrorDesc: "Leader election not needed for topic partition.",
	}
	AllError[85] = &Error{
		ErrorMsg:  "NO_REASSIGNMENT_IN_PROGRESS",
		ErrorCode: 85,
		Retriable: false,
		ErrorDesc: "No partition reassignment is in progress.",
	}
	AllError[86] = &Error{
		ErrorMsg:  "GROUP_SUBSCRIBED_TO_TOPIC",
		ErrorCode: 86,
		Retriable: false,
		ErrorDesc: "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.",
	}
	AllError[87] = &Error{
		ErrorMsg:  "INVALID_RECORD",
		ErrorCode: 87,
		Retriable: false,
		ErrorDesc: "This record has failed the validation on broker and hence will be rejected.",
	}
	AllError[88] = &Error{
		ErrorMsg:  "UNSTABLE_OFFSET_COMMIT",
		ErrorCode: 88,
		Retriable: true,
		ErrorDesc: "There are unstable offsets that need to be cleared.",
	}
	AllError[89] = &Error{
		ErrorMsg:  "THROTTLING_QUOTA_EXCEEDED",
		ErrorCode: 89,
		Retriable: true,
		ErrorDesc: "The throttling quota has been exceeded.",
	}
	AllError[90] = &Error{
		ErrorMsg:  "PRODUCER_FENCED",
		ErrorCode: 90,
		Retriable: false,
		ErrorDesc: "There is a newer producer with the same transactionalId which fences the current one.",
	}
	AllError[91] = &Error{
		ErrorMsg:  "RESOURCE_NOT_FOUND",
		ErrorCode: 91,
		Retriable: false,
		ErrorDesc: "A request illegally referred to a resource that does not exist.",
	}
	AllError[92] = &Error{
		ErrorMsg:  "DUPLICATE_RESOURCE",
		ErrorCode: 92,
		Retriable: false,
		ErrorDesc: "A request illegally referred to the same resource twice.",
	}
	AllError[93] = &Error{
		ErrorMsg:  "UNACCEPTABLE_CREDENTIAL",
		ErrorCode: 93,
		Retriable: false,
		ErrorDesc: "Requested credential would not meet criteria for acceptability.",
	}
	AllError[94] = &Error{
		ErrorMsg:  "INCONSISTENT_VOTER_SET",
		ErrorCode: 94,
		Retriable: false,
		ErrorDesc: "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters",
	}
	AllError[95] = &Error{
		ErrorMsg:  "INVALID_UPDATE_VERSION",
		ErrorCode: 95,
		Retriable: false,
		ErrorDesc: "The given update version was invalid.",
	}
	AllError[96] = &Error{
		ErrorMsg:  "FEATURE_UPDATE_FAILED",
		ErrorCode: 96,
		Retriable: false,
		ErrorDesc: "Unable to update finalized features due to an unexpected server error.",
	}
	AllError[97] = &Error{
		ErrorMsg:  "PRINCIPAL_DESERIALIZATION_FAILURE",
		ErrorCode: 97,
		Retriable: false,
		ErrorDesc: "Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.",
	}
	AllError[98] = &Error{
		ErrorMsg:  "SNAPSHOT_NOT_FOUND",
		ErrorCode: 98,
		Retriable: false,
		ErrorDesc: "Requested snapshot was not found",
	}
	AllError[99] = &Error{
		ErrorMsg:  "POSITION_OUT_OF_RANGE",
		ErrorCode: 99,
		Retriable: false,
		ErrorDesc: "Requested position is not greater than or equal to zero, and less than the size of the snapshot.",
	}
	AllError[100] = &Error{
		ErrorMsg:  "UNKNOWN_TOPIC_ID",
		ErrorCode: 100,
		Retriable: true,
		ErrorDesc: "This server does not host this topic ID.",
	}
	AllError[101] = &Error{
		ErrorMsg:  "DUPLICATE_BROKER_REGISTRATION",
		ErrorCode: 101,
		Retriable: false,
		ErrorDesc: "This broker ID is already in use.",
	}
	AllError[102] = &Error{
		ErrorMsg:  "BROKER_ID_NOT_REGISTERED",
		ErrorCode: 102,
		Retriable: false,
		ErrorDesc: "The given broker ID was not registered.",
	}
	AllError[103] = &Error{
		ErrorMsg:  "INCONSISTENT_TOPIC_ID",
		ErrorCode: 103,
		Retriable: true,
		ErrorDesc: "The log's topic ID did not match the topic ID in the request",
	}
	AllError[104] = &Error{
		ErrorMsg:  "INCONSISTENT_CLUSTER_ID",
		ErrorCode: 104,
		Retriable: false,
		ErrorDesc: "The clusterId in the request does not match that found on the server",
	}
	AllError[105] = &Error{
		ErrorMsg:  "TRANSACTIONAL_ID_NOT_FOUND",
		ErrorCode: 105,
		Retriable: false,
		ErrorDesc: "The transactionalId could not be found",
	}
	AllError[106] = &Error{
		ErrorMsg:  "FETCH_SESSION_TOPIC_ID_ERROR",
		ErrorCode: 106,
		Retriable: true,
		ErrorDesc: "The fetch session encountered inconsistent topic ID usage",
	}
	AllError[107] = &Error{
		ErrorMsg:  "INELIGIBLE_REPLICA",
		ErrorCode: 107,
		Retriable: false,
		ErrorDesc: "The new ISR contains at least one ineligible replica.",
	}
	AllError[108] = &Error{
		ErrorMsg:  "NEW_LEADER_ELECTED",
		ErrorCode: 108,
		Retriable: false,
		ErrorDesc: "The AlterPartition request successfully updated the partition state but the leader has changed.",
	}
	AllError[109] = &Error{
		ErrorMsg:  "OFFSET_MOVED_TO_TIERED_STORAGE",
		ErrorCode: 109,
		Retriable: false,
		ErrorDesc: "The requested offset is moved to tiered storage.",
	}
	AllError[110] = &Error{
		ErrorMsg:  "FENCED_MEMBER_EPOCH",
		ErrorCode: 110,
		Retriable: false,
		ErrorDesc: "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.",
	}
	AllError[111] = &Error{
		ErrorMsg:  "UNRELEASED_INSTANCE_ID",
		ErrorCode: 111,
		Retriable: false,
		ErrorDesc: "The instance ID is still used by another member in the consumer group. That member must leave first.",
	}
	AllError[112] = &Error{
		ErrorMsg:  "UNSUPPORTED_ASSIGNOR",
		ErrorCode: 112,
		Retriable: false,
		ErrorDesc: "The assignor or its version range is not supported by the consumer group.",
	}
	AllError[113] = &Error{
		ErrorMsg:  "STALE_MEMBER_EPOCH",
		ErrorCode: 113,
		Retriable: false,
		ErrorDesc: "The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.",
	}
	AllError[114] = &Error{
		ErrorMsg:  "MISMATCHED_ENDPOINT_TYPE",
		ErrorCode: 114,
		Retriable: false,
		ErrorDesc: "The request was sent to an endpoint of the wrong type.",
	}
	AllError[115] = &Error{
		ErrorMsg:  "UNSUPPORTED_ENDPOINT_TYPE",
		ErrorCode: 115,
		Retriable: false,
		ErrorDesc: "This endpoint type is not supported yet.",
	}
	AllError[116] = &Error{
		ErrorMsg:  "UNKNOWN_CONTROLLER_ID",
		ErrorCode: 116,
		Retriable: false,
		ErrorDesc: "This controller ID is not known.",
	}
	AllError[117] = &Error{
		ErrorMsg:  "UNKNOWN_SUBSCRIPTION_ID",
		ErrorCode: 117,
		Retriable: false,
		ErrorDesc: "Client sent a push telemetry request with an invalid or outdated subscription ID.",
	}
	AllError[118] = &Error{
		ErrorMsg:  "TELEMETRY_TOO_LARGE",
		ErrorCode: 118,
		Retriable: false,
		ErrorDesc: "Client sent a push telemetry request larger than the maximum size the broker will accept.",
	}
	AllError[118] = &Error{
		ErrorMsg:  "INVALID_REGISTRATION",
		ErrorCode: 119,
		Retriable: false,
		ErrorDesc: "The controller has considered the broker registration to be invalid.",
	}
}
