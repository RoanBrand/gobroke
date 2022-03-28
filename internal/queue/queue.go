package queue

import (
	"sync"
	"sync/atomic"
	"time"
)

// Base message queue.
type queue struct {
	h, t *Item
	sync.Mutex
}

// Inbound PUBLISH messages to server, as well as outbound QoS 0 to clients.
type Basic struct {
	queue
	trig *sync.Cond
}

// Outbound QoS 1 & 2 PUBLISH messages to clients.
type QoS12 struct {
	Basic
	lookup map[uint16]*Item
	toSend *Item
}

// Outbound PUBREL messages,
type QoS2Part2 struct {
	queue
	lookup map[uint16]*Item
}

func (q *Basic) Init() {
	q.trig = sync.NewCond(q)
}

func (q *QoS12) Init() {
	q.Basic.Init()
	q.lookup = make(map[uint16]*Item)
}

func (q *QoS2Part2) Init() {
	q.lookup = make(map[uint16]*Item)
}

func (q *Basic) Reset() {
	q.Lock()
	q.h, q.t = nil, nil
	q.Unlock()
}

func (q *QoS12) Reset() {
	q.Lock()
	for i := range q.lookup {
		delete(q.lookup, i)
	}
	q.h, q.t = nil, nil
	q.Unlock()
}

func (q *QoS2Part2) Reset() {
	q.Lock()
	for i := range q.lookup {
		delete(q.lookup, i)
	}
	q.h, q.t = nil, nil
	q.Unlock()
}

func (q *queue) add(i *Item) {
	if q.h == nil {
		q.h = i
		q.t = i
	} else {
		q.t.next = i
		i.prev = q.t
		q.t = i
	}
}

func (q *queue) remove(i *Item) {
	if i.prev == nil { // is h
		q.h = i.next
	} else {
		i.prev.next = i.next
	}

	if i.next == nil { // is t
		q.t = i.prev
	} else {
		i.next.prev = i.prev
	}

	i.prev, i.next = nil, nil // avoid memory leaks
}

// Inbound, and Outbound QoS 0.
func (q *Basic) Add(i *Item) {
	q.Lock()
	q.add(i)
	q.NotifyDispatcher()
	q.Unlock()
}

// Outbound QoS 1 & 2.
func (q *QoS12) Add(i *Item) {
	q.Lock()
	q.add(i)
	q.lookup[i.PId] = i
	if q.toSend == nil {
		q.toSend = i
	}
	q.NotifyDispatcher()
	q.Unlock()
}

// Outbound QoS 2 PUBREL messages.
func (q *QoS2Part2) Add(i *Item) {
	i.Sent = time.Now()
	q.Lock()
	if _, ok := q.lookup[i.PId]; !ok {
		q.add(i)
		q.lookup[i.PId] = i
	}
	q.Unlock()
}

// Remove finalized QoS 1 & 2 messages.
func (q *QoS12) Remove(id uint16) *Item {
	q.Lock()
	if i, ok := q.lookup[id]; ok {
		q.remove(i)
		delete(q.lookup, id)
		q.Unlock()
		return i
	}
	q.Unlock()
	return nil
}

// Remove PUBREL messages that were finalized.
func (q *QoS2Part2) Remove(id uint16) *Item {
	q.Lock()
	if i, ok := q.lookup[id]; ok {
		q.remove(i)
		delete(q.lookup, id)
		q.Unlock()
		return i
	}
	q.Unlock()
	return nil
}

// NotifyDispatcher will signal dispatcher to check the queue.
func (q *Basic) NotifyDispatcher() {
	q.trig.Signal()
}

// StartDispatcher will continuously dispatch queue items and remove them.
func (q *Basic) StartDispatcher(d func(*Item) error, killed *int32, wg *sync.WaitGroup) {
	defer func() {
		if wg != nil {
			wg.Done()
		}
	}()
	for {
		q.Lock()
		if killed != nil && atomic.LoadInt32(killed) == 1 {
			q.Unlock()
			return
		}
		if q.h == nil {
			q.trig.Wait()
		}
		if killed != nil && atomic.LoadInt32(killed) == 1 {
			q.Unlock()
			return
		}

		i := q.h
		if i != nil {
			q.h = i.next
			if q.h == nil {
				q.t = nil
			} else {
				i.next = nil // avoid memory leakage
				q.h.prev = nil
			}
		}
		q.Unlock()

		if i != nil {
			if err := d(i); err != nil {
				return
			}
		}
	}
}

// StartDispatcher will continuously dispatch new, previously undispatched queue items. Doesn't remove them.
func (q *QoS12) StartDispatcher(d func(*Item) error, killed *int32, wg *sync.WaitGroup) {
	defer func() {
		wg.Done()
	}()
	for {
		q.Lock()
		if atomic.LoadInt32(killed) == 1 {
			q.Unlock()
			return
		}
		if q.toSend == nil {
			q.trig.Wait()
		}
		if atomic.LoadInt32(killed) == 1 {
			q.Unlock()
			return
		}

		i := q.toSend
		if i != nil {
			q.toSend = i.next
		}
		q.Unlock()

		if i != nil {
			if err := d(i); err != nil {
				return
			}
			i.Sent = time.Now() // may race with monitor
		}
	}
}

// Resend pending/unacknowledged QoS 1 & 2 messages after timeout.
func (q *queue) monitorTimeouts(toMS uint64, killed chan struct{}, timedOut func(*Item) error) {
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
			for i := q.h; i != nil; i = i.next {
				if i.Sent.IsZero() {
					break // if we encounter unsent, stop
				}
				if pending := now.Sub(i.Sent); pending < timeout {
					if candidate := timeout - pending; candidate < nextRun {
						nextRun = candidate
					}
					continue
				}

				if err := timedOut(i); err != nil {
					q.Unlock()
					return
				}

				i.Sent = now
			}
			q.Unlock()

			t.Reset(nextRun)
		}
	}
}

// Resend pending/unacknowledged QoS 1 & 2 PUBLISHs after timeout.
func (q *QoS12) MonitorTimeouts(toMS uint64, d func(*Item) error, killed chan struct{}, wg *sync.WaitGroup) {
	q.queue.monitorTimeouts(toMS, killed, func(i *Item) error {
		return d(i)
	})
	wg.Done()
}

// Resend pending/unacknowledged QoS 2 PUBRELs after timeout.
func (q *QoS2Part2) MonitorTimeouts(toMS uint64, d func(*Item) error, killed chan struct{}, wg *sync.WaitGroup) {
	q.queue.monitorTimeouts(toMS, killed, func(i *Item) error {
		return d(i)
	})
	wg.Done()
}

// Resend all pending QoS 1 & 2 PUBLISHs for new session.
func (q *QoS12) ResendAll(d func(*Item) error) error {
	q.Lock()
	for i := q.h; i != nil; i = i.next {
		if err := d(i); err != nil {
			q.Unlock()
			return err
		}

		q.toSend = i.next
		i.Sent = time.Now()
	}
	q.Unlock()
	return nil
}

// Resend all pending QoS 2 PUBRELs for new session.
func (q *QoS2Part2) ResendAll(d func(*Item) error) error {
	q.Lock()
	for i := q.h; i != nil; i = i.next {
		if err := d(i); err != nil {
			q.Unlock()
			return err
		}

		i.Sent = time.Now()
	}
	q.Unlock()
	return nil
}
