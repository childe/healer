package healer

import (
	"flag"
	"strconv"
	"strings"
)

var kafkaVersion = flag.String("kafka-version", "1.0.0", "certain kafka API version is dependent on the flag")

// compareKafkaVersion("0.9.0", "0.10.0") == -1
func compareKafkaVersion(A, B string) int {
	splitedA := strings.Split(A, ".")
	splitedB := strings.Split(B, ".")

	var (
		n   int
		nn  int
		err error
	)

	for i, part := range splitedA {
		if n, err = strconv.Atoi(part); err != nil {
			panic(err)
		}
		if i >= len(splitedB) {
			if n > 0 {
				return 1
			} else {
				continue
			}
		}

		if nn, err = strconv.Atoi(splitedB[i]); err != nil {
			panic(err)
		}

		if n > nn {
			return 1
		}
		if n < nn {
			return -1
		}
		continue
	}

	return 0
}
