package gokafka

import "testing"

func TestGetmetadata(t *testing.T) {
	_, err := GetMetaData("kafka.test:9092", "test", 0, "gokafka")
	if err != nil {
		t.Error("could not get metadata of topic(test) from server(kafka.test)")
		t.Error(err)
	}

	_, err = GetMetaData("kafka.test:80", "test", 0, "gokafka")
	if err != nil {
		t.Log("could not get metadata of topic(test) from 80 port")
	}
}

func TestGetoffset(t *testing.T) {
	_, err := GetOffset("kafka.test:9092", "test", 0, 0, "gokafka", -1, 1)
	if err != nil {
		t.Error("could not get metadata of topic(test) from server(kafka.test)")
		t.Error(err)
	}

	_, err = GetMetaData("kafka.test:80", "test", 0, "gokafka")
	if err != nil {
		t.Log("could not get metadata of topic(test) from 80 port")
	}
}
