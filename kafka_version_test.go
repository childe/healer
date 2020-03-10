package healer

import "testing"

func TestKafkaVersionCompare(t *testing.T) {
	var a string
	var b string

	a = "0.9.0"
	b = "0.9.1"
	if compareKafkaVersion(a, b) != -1 {
		t.Errorf("%s >= %s", a, b)
	}

	a = "0.9.0"
	b = "1.0.0"
	if compareKafkaVersion(a, b) != -1 {
		t.Errorf("%s >= %s", a, b)
	}

	a = "0.9.0"
	b = "0.9.0.0"
	if compareKafkaVersion(a, b) != 0 {
		t.Errorf("%s != %s", a, b)
	}

	a = "1.0.0"
	b = "0.9.1.0"
	if compareKafkaVersion(a, b) != 1 {
		t.Errorf("%s <= %s", a, b)
	}
}
