package queue

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// Base message queue.
type queue struct {
	sync.Mutex
	l *list.List
}

// PUBLISH messages
type QoS0 struct {
	queue
	trig *sync.Cond
}

type QoS12 struct {
	QoS0
	lookup map[uint16]*list.Element
	toSend *list.Element
}

// PUBREL messages
type QoS2Part2 struct {
	queue
	lookup map[uint16]*list.Element
	pubRel []byte
}

func (q *QoS0) Init() {
	q.l = list.New()
	q.trig = sync.NewCond(q)
}

func (q *QoS12) Init() {
	q.QoS0.Init()
	q.lookup = make(map[uint16]*list.Element, 2)
}

func (q *QoS2Part2) Init(pubRel []byte) {
	q.l = list.New()
	q.lookup = make(map[uint16]*list.Element, 2)
	q.pubRel = pubRel
}

func (q *QoS0) Reset() {
	q.Lock()
	q.l.Init()
	q.Unlock()
}

func (q *QoS12) Reset() {
	q.Lock()
	for i := range q.lookup {
		delete(q.lookup, i)
	}
	q.l.Init()
	q.Unlock()
}

func (q *QoS2Part2) Reset() {
	q.Lock()
	for i := range q.lookup {
		delete(q.lookup, i)
	}
	q.l.Init()
	q.Unlock()
}

// Queue element/item.
type msg struct {
	sent time.Time
	p    []byte // PUBLISH
	id   uint16 // PUBREL
}

// Store outbound QoS 0 PUBLISH messages for client.
func (q *QoS0) Add(p []byte) {
	q.Lock()
	q.l.PushBack(p)
	q.trig.Signal()
	q.Unlock()
}

// Store outbound QoS 1&2 PUBLISH messages.
func (q *QoS12) Add(id uint16, p []byte) {
	q.Lock()
	q.lookup[id] = q.l.PushBack(&msg{p: p})
	if q.toSend == nil {
		q.toSend = q.lookup[id]
	}
	q.trig.Signal()
	q.Unlock()
}

// Store outbound QoS 2 PUBREL messages.
func (q *QoS2Part2) Add(id uint16) {
	q.Lock()
	if _, ok := q.lookup[id]; !ok {
		pubR := &msg{sent: time.Now(), id: id}
		q.lookup[id] = q.l.PushBack(pubR)
	}
	q.Unlock()
}

// Remove finalized QoS 1&2 messages.
func (q *QoS12) Remove(id uint16) bool {
	q.Lock()
	if qPub, ok := q.lookup[id]; ok {
		q.l.Remove(qPub)
		delete(q.lookup, id)
		q.Unlock()
		return true
	}
	q.Unlock()
	return false
}

// Remove PUBREL messages that were finalized.
func (q *QoS2Part2) Remove(id uint16) bool {
	q.Lock()
	if qPub, ok := q.lookup[id]; ok {
		q.l.Remove(qPub)
		delete(q.lookup, id)
		q.Unlock()
		return true
	}
	q.Unlock()
	return false
}

// Signal to check list and if killed.
func (q *QoS0) Signal() {
	q.trig.Signal()
}

// Worker routine for QoS 0 that sends new messages to client.
// It will remove a message once it is has been successfully sent to the client.
func (q *QoS0) StartDispatcher(write func([]byte) error, killed *int32) {
	defer q.Unlock()
	for {
		q.Lock()
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
		q.Unlock()
	}
}

// Worker routine for QoS 1&2 that sends new messages to client.
func (q *QoS12) StartDispatcher(write func([]byte) error, killed *int32) {
	defer q.Unlock()
	for {
		q.Lock()
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
		}
		q.Unlock()
	}
}

// Resend pending/unacknowledged QoS>0 messages after timeout.
func (q *queue) monitorTimeouts(toMS uint64, killed chan struct{}, timedOut func(m *msg) error) {
	timeout := time.Millisecond * time.Duration(toMS)
	t := time.NewTimer(timeout)
	defer func() {
		if !t.Stop() {
			<-t.C
		}
	}()

	for {
		select {
		case <-killed:
			return
		case now := <-t.C:
			nextRun := timeout

			q.Lock()
			for p := q.l.Front(); p != nil; p = p.Next() {
				m := p.Value.(*msg)
				if m.sent.IsZero() {
					break // if we encounter unsent, stop
				}
				if pending := now.Sub(m.sent); pending < timeout {
					if candidate := timeout - pending; candidate < nextRun {
						nextRun = candidate
					}
					continue
				}

				if err := timedOut(m); err != nil {
					q.Unlock()
					return
				}

				m.sent = now
			}
			q.Unlock()

			t.Reset(nextRun)
		}
	}
}

// Resend pending/unacknowledged QoS 1,2 PUBLISHs after timeout.
func (q *QoS12) MonitorTimeouts(toMS uint64, write func([]byte) error, killed chan struct{}) {
	q.queue.monitorTimeouts(toMS, killed, func(m *msg) error {
		setDUPFlag(m.p)
		return write(m.p)
	})
}

// Resend pending/unacknowledged QoS 2 PUBRELs after timeout.
func (q *QoS2Part2) MonitorTimeouts(toMS uint64, write func([]byte) error, killed chan struct{}) {
	q.queue.monitorTimeouts(toMS, killed, func(m *msg) error {
		q.pubRel[2], q.pubRel[3] = byte(m.id>>8), byte(m.id)
		return write(q.pubRel)
	})
}

// Resend all pending QoS 1&2 PUBLISHs for new session.
// Messages that have been sent before get sent as DUPs.
func (q *QoS12) ResendAll(write func([]byte) error) error {
	q.Lock()
	for p := q.l.Front(); p != nil; p = p.Next() {
		m := p.Value.(*msg)
		if !m.sent.IsZero() {
			setDUPFlag(m.p)
		}
		if err := write(m.p); err != nil {
			q.Unlock()
			return err
		}

		q.toSend = p.Next()
		m.sent = time.Now()
	}
	q.Unlock()
	return nil
}

// Resend all pending QoS 2 PUBRELs for new session.
func (q *QoS2Part2) ResendAll(write func([]byte) error) error {
	q.Lock()
	for pr := q.l.Front(); pr != nil; pr = pr.Next() {
		m := pr.Value.(*msg)
		q.pubRel[2], q.pubRel[3] = byte(m.id>>8), byte(m.id)
		if err := write(q.pubRel); err != nil {
			q.Unlock()
			return err
		}

		m.sent = time.Now()
	}
	q.Unlock()
	return nil
}

// Indicate that outbound message has been sent to client in the past.
func setDUPFlag(pub []byte) {
	pub[0] |= 0x08
}
