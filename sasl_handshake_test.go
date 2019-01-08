package healer

import "testing"

func TestGenSaslHandShakeRequest(t *testing.T) {
	var (
		clientID  string = "healer"
		mechanism string = ""
	)

	saslHandShakeRequest := NewSaslHandShakeRequest(clientID, mechanism)

	payload := saslHandShakeRequest.Encode()
	if len(payload) != 54 {
		t.Error("SaslHandShakeRequest payload length should be 54")
	}
}
