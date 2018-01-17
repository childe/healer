package healer

type HealerError int32

func (healerError *HealerError) Error() string {
	switch *healerError {
	case 1:
		return "max.bytes parameter is too small. This size must be at least as large as the maximum message size the server allows or else it is possible for the producer to send messages larger than the consumer can fetch. If that happens, the consumer can get stuck trying to fetch a large message on a certain partition."
	case 2:
		return "NO partitionResponse?"
	}
	return ""
}

var (
	maxBytesTooSmall    HealerError = 1
	noPartitionResponse HealerError = 2
)
