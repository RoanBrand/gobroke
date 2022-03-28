package model

// PubMessage is a data message from a client published to the Server.
// TODO: make only single []byte
type PubMessage struct {
	Raw    []byte // topicUTF8 + payload
	RxQoS  uint8  // qos of received msg
	Retain bool   // msg received by server to be retained
}
