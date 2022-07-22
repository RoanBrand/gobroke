package model

import (
	"sync"
	"sync/atomic"
)

// PubMessage is a data message from a client published to the Server.
type PubMessage struct {
	Refs int32

	// flags + topicUTF8 + payload
	// byte 0: publish flags, i.e. DUP, QoS, Retain
	// byte 1 to 3+topicLen: topicUTF8
	// byte 3+topicLen to 3+topicLen+len(payload): payload
	Pub []byte
}

var pool = sync.Pool{
	New: func() any {
		return new(PubMessage)
	},
}

func NewPub(flags uint8, topicUTF8, payload []byte) *PubMessage {
	p := pool.Get().(*PubMessage)
	p.Refs = 1
	if p.Pub == nil {
		p.Pub = make([]byte, 1, 1+len(topicUTF8)+len(payload))
	}

	p.Pub[0] = flags
	p.Pub = append(p.Pub, topicUTF8...)
	p.Pub = append(p.Pub, payload...)
	return p
}

func (p *PubMessage) AddUser() {
	atomic.AddInt32(&p.Refs, 1)
}

func (p *PubMessage) FreeIfLastUser() {
	if atomic.AddInt32(&p.Refs, -1) > 0 {
		return
	}

	p.Pub = p.Pub[:1]
	pool.Put(p)
}

func (p *PubMessage) Duplicate() bool {
	return p.Pub[0]&0x08 > 0 // msg received by server is DUP
}

func (p *PubMessage) RxQoS() uint8 {
	return (p.Pub[0] & 0x06) >> 1 // qos of received msg
}

func (p *PubMessage) ToRetain() bool {
	return p.Pub[0]&0x01 > 0 // msg received by server to be retained
}
