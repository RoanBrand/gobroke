package model

// PubMessage is a data message from a client published to the Server.
type PubMessage []byte // flags + topicUTF8 + payload

// byte 0: publish flags, i.e. DUP, QoS, Retain
// byte 1 to 3+topicLen: topicUTF8
// byte 3+topicLen to 3+topicLen+len(payload): payload
func MakePub(flags uint8, topicUTF8, payload []byte) (p PubMessage) {
	p = make(PubMessage, 1, 1+len(topicUTF8)+len(payload))
	p[0] = flags
	p = append(p, topicUTF8...)
	p = append(p, payload...)
	return
}

func (p PubMessage) Duplicate() bool {
	return p[0]&0x08 > 0 // msg received by server is DUP
}

func (p PubMessage) RxQoS() uint8 {
	return (p[0] & 0x06) >> 1 // qos of received msg
}

func (p PubMessage) Retain() bool {
	return p[0]&0x01 > 0 // msg received by server to be retained
}
