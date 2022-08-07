package model

import (
	"sync"
	"sync/atomic"
)

// PubMessage is a data message from a client published to the Server.
type PubMessage struct {
	refs int32

	// flags + topicUTF8 + payload
	// byte 0: publish flags, i.e. DUP, QoS, Retain
	// byte 1 to 3+topicLen: topicUTF8
	// byte 3+topicLen to 3+topicLen+len(payload): payload
	Pub []byte

	Publisher string
}

var pool = sync.Pool{}

func NewPub(flags uint8, topicUTF8, payload []byte) (p *PubMessage) {
	if pi := pool.Get(); pi == nil {
		p = &PubMessage{Pub: make([]byte, 1, 1+len(topicUTF8)+len(payload))}
	} else {
		p = pi.(*PubMessage)
		p.Pub = p.Pub[:1]
	}

	p.refs = 1
	p.Pub[0] = flags
	p.Pub = append(p.Pub, topicUTF8...)
	p.Pub = append(p.Pub, payload...)
	return p
}

func (p *PubMessage) AddUser() {
	atomic.AddInt32(&p.refs, 1)
}

func (p *PubMessage) FreeIfLastUser() {
	if atomic.AddInt32(&p.refs, -1) > 0 {
		return
	}

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
