package healer

const (
	UNKNOWN int8 = iota

	/**
	 * In a filter matches any ResourceType.
	 */
	ANY

	/**
	 * A Kafka topic.
	 */
	TOPIC

	/**
	 * A consumer group.
	 */
	GROUP

	/**
	 * The cluster as a whole.
	 */
	CLUSTER

	/**
	 * A transactional ID.
	 */
	TRANSACTIONAL_ID

	/**
	 * A token ID.
	 */
	DELEGATION_TOKEN
)
