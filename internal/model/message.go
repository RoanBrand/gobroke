package model

import (
	"sync"
	"sync/atomic"
)

// PubMessage represents a PUBLISH packet and its content.
type PubMessage struct {
	Expiry int64
	refs   int32

	// flags + topicUTF8 + payload
	// byte 0: publish flags, i.e. DUP, QoS, Retain
	// byte 1: topicUTF8
	// byte 3+topicLen: payload
	B []byte

	Props []byte // Properties to be forwarded as is

	Publisher string // ClientId
}

var pool = sync.Pool{}

func NewPub(requiredBLen int) (p *PubMessage) {
	if pi := pool.Get(); pi == nil {
		p = &PubMessage{B: make([]byte, 1, requiredBLen)}
	} else {
		p = pi.(*PubMessage)
		if cap(p.B) < requiredBLen {
			p.B = make([]byte, 1, requiredBLen)
		} else {
			p.B = p.B[:1]
		}
	}

	p.Expiry = 0
	p.refs = 1
	return p
}

func NewPubOld(flags uint8, topicUTF8, payload []byte) (p *PubMessage) {
	p = NewPub(1 + len(topicUTF8) + len(payload))
	p.B[0] = flags
	p.B = append(p.B, topicUTF8...)
	p.B = append(p.B, payload...)
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
	return p.B[0]&0x08 > 0 // msg received by server is DUP
}

func (p *PubMessage) RxQoS() uint8 {
	return (p.B[0] & 0x06) >> 1 // qos of received msg
}

func (p *PubMessage) ToRetain() bool {
	return p.B[0]&0x01 > 0 // msg received by server to be retained
}
