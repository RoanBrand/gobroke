package queue

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// PUBLISH messages
type QoS0 struct {
	lock sync.Mutex
	l    *list.List
	trig *sync.Cond
}

type QoS12 struct {
	QoS0
	lookup map[uint16]*list.Element
	toSend *list.Element
}

// PUBREL messages
type QoS2Part2 struct {
	lock   sync.Mutex
	l      *list.List
	lookup map[uint16]*list.Element
}

func (q *QoS0) Init() {
	q.l = list.New()
	q.trig = sync.NewCond(&q.lock)
}

func (q *QoS12) Init() {
	q.QoS0.Init()
	q.lookup = make(map[uint16]*list.Element, 2)
}

func (q *QoS2Part2) Init() {
	q.l = list.New()
	q.lookup = make(map[uint16]*list.Element, 2)
}

func (q *QoS0) Reset() {
	q.lock.Lock()
	q.l.Init()
	q.lock.Unlock()
}

func (q *QoS12) Reset() {
	q.lock.Lock()
	for i, el := range q.lookup {
		close(el.Value.(*msg).done)
		delete(q.lookup, i)
	}
	q.l.Init()
	q.lock.Unlock()
}

func (q *QoS2Part2) Reset() {
	q.lock.Lock()
	for i, el := range q.lookup {
		close(el.Value.(*msg).done)
		delete(q.lookup, i)
	}
	q.l.Init()
	q.lock.Unlock()
}

type msg struct {
	done chan struct{}
	p    []byte
	sent time.Time
}

// Store outbound QoS 0 PUBLISH messages for client.
func (q *QoS0) Add(p []byte) {
	q.lock.Lock()
	q.l.PushBack(p)
	q.trig.Signal()
	q.lock.Unlock()
}

// Store outbound QoS 1&2 PUBLISH messages.
func (q *QoS12) Add(id uint16, p []byte) {
	q.lock.Lock()
	q.lookup[id] = q.l.PushBack(&msg{done: make(chan struct{}), p: p})
	if q.toSend == nil {
		q.toSend = q.lookup[id]
	}
	q.trig.Signal()
	q.lock.Unlock()
}

// Store outbound QoS 2 PUBREL messages.
func (q *QoS2Part2) Add(id uint16, pubRel []byte, write func([]byte) error) {
	q.lock.Lock()
	if _, ok := q.lookup[id]; !ok {
		pubR := &msg{done: make(chan struct{}), p: pubRel}
		q.lookup[id] = q.l.PushBack(pubR)
		go monitor(write, pubR, false)
	}
	q.lock.Unlock()
}

// Remove messages that were successfully sent (QoS 0), or that got finalized (QoS 1&2).
// Also signal message monitor to exit.
func (q *QoS12) Remove(id uint16) bool {
	q.lock.Lock()
	if qPub, ok := q.lookup[id]; ok {
		close(q.l.Remove(qPub).(*msg).done)
		delete(q.lookup, id)
		q.lock.Unlock()
		return true
	}
	q.lock.Unlock()
	return false
}

// Remove PUBREL messages that where finalized.
func (q *QoS2Part2) Remove(id uint16) bool {
	q.lock.Lock()
	if qPub, ok := q.lookup[id]; ok {
		close(q.l.Remove(qPub).(*msg).done)
		delete(q.lookup, id)
		q.lock.Unlock()
		return true
	}
	q.lock.Unlock()
	return false
}

// Signal to check list and if killed.
func (q *QoS0) Signal() {
	q.trig.Signal()
}

// Worker routine for QoS 0 that sends new messages to client.
// It will remove a message once it is has been successfully sent to the client.
func (q *QoS0) StartDispatcher(write func([]byte) error, killed *int32) {
	defer q.lock.Unlock()
	for {
		q.lock.Lock()
		if atomic.LoadInt32(killed) == 1 {
			return
		}
		if q.l.Front() == nil {
			q.trig.Wait()
		}
		if atomic.LoadInt32(killed) == 1 {
			return
		}
		for p := q.l.Front(); p != nil; p = p.Next() {
			if err := write(p.Value.([]byte)); err != nil {
				return
			}
			q.l.Remove(p)
		}
		q.lock.Unlock()
	}
}

// Worker routine for QoS 1&2 that sends new messages to client.
// A new routine will also resend all messages still in queue as duplicates.
func (q *QoS12) StartDispatcher(write func([]byte) error, killed *int32) {
	q.lock.Lock()
	for p := q.l.Front(); p != nil; p = p.Next() {
		m := p.Value.(*msg)
		if !m.sent.IsZero() {
			setDUPFlag(m.p)
		}
		if err := write(m.p); err != nil {
			return
		}

		m.sent = time.Now()
		go monitor(write, m, true)
	}
	q.lock.Unlock()

	defer q.lock.Unlock()
	for {
		q.lock.Lock()
		if atomic.LoadInt32(killed) == 1 {
			return
		}

		if q.toSend == nil {
			q.trig.Wait()
		}
		if atomic.LoadInt32(killed) == 1 {
			return
		}

		for p := q.toSend; p != nil; p = p.Next() {
			m := p.Value.(*msg)
			if err := write(m.p); err != nil {
				return
			}

			q.toSend = p.Next()
			m.sent = time.Now()
			go monitor(write, m, true)
		}

		q.lock.Unlock()
	}
}

// Monitor pending QoS 1&2 message, resend after timeout.
// Exit if message has been acknowledged and removed from queue.
// Set isPub to false if PUBREL, so not to set DUP on resend.
func monitor(write func([]byte) error, m *msg, isPub bool) {
	t := time.NewTimer(time.Second * 20) // correct value or method?
	for {
		select {
		case <-m.done:
			if !t.Stop() {
				<-t.C
			}
			return
		case <-t.C:
			if isPub {
				setDUPFlag(m.p)
			}
			if err := write(m.p); err != nil {
				return
			}
			t.Reset(time.Second * 20)
		}
	}
}

// Resend all QoS 2 PUBRELs.
func (q *QoS2Part2) ResendAll(write func([]byte) error) {
	q.lock.Lock()
	for pr := q.l.Front(); pr != nil; pr = pr.Next() {
		m := pr.Value.(*msg)
		if err := write(m.p); err != nil {
			return
		}
		go monitor(write, m, false)
	}
	q.lock.Unlock()
}

// Indicate that outbound message has been sent to client in the past.
func setDUPFlag(pub []byte) {
	pub[0] |= 0x08
}
