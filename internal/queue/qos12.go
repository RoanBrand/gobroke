package queue

import (
	"context"
	"sync"
)

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

func (q *QoS12) Init() {
	q.Basic.Init()
	q.lookup = make(map[uint16]*Item)
}

func (q *QoS2Part2) Init() {
	q.lookup = make(map[uint16]*Item)
}

func (q *QoS12) Add(i *Item) {
	q.Lock()
	q.add(i)
	//q.lookup[i.PId] = i
	if q.toSend == nil {
		q.toSend = i
	}
	q.Unlock()
	q.NotifyDispatcher()
}

// Outbound QoS 2 PUBREL messages.
func (q *QoS2Part2) Add(i *Item) {
	q.Lock()
	if _, ok := q.lookup[i.PId]; !ok {
		q.add(i)
		q.lookup[i.PId] = i
	} else {
		ReturnItem(i)
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

func (q *QoS12) Reset() {
	q.Lock()
	for id, i := range q.lookup {
		q.remove(i)
		delete(q.lookup, id)
		i.P.FreeIfLastUser()
		ReturnItemQos12(i)
	}
	q.h, q.t = nil, nil
	q.Unlock()
}

func (q *QoS2Part2) Reset() {
	q.Lock()
	for id, i := range q.lookup {
		q.remove(i)
		delete(q.lookup, id)
		ReturnItemQos12(i)
	}
	q.h, q.t = nil, nil
	q.Unlock()
}

// Present checks if QoS2 msg in stage 2 of transmission.
func (q *QoS2Part2) Present(id uint16) bool {
	q.Lock()
	_, ok := q.lookup[id]
	q.Unlock()
	return ok
}

// StartDispatcher will continuously dispatch new, previously undispatched queue items. Doesn't remove them.
func (q *QoS12) StartDispatcher(ctx context.Context, d func(*Item) error, getPId func() uint16, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		q.Lock()

		if ctx.Err() != nil {
			q.Unlock()
			return
		}

		if q.toSend == nil {
			q.trig.Wait()
		}

		i := q.toSend
		if i != nil {
			q.toSend = i.next
		}

		q.Unlock()

		if i != nil {
			i.PId = getPId()

			q.Lock()
			q.lookup[i.PId] = i
			q.Unlock()

			if err := d(i); err != nil {
				return
			}
		}
	}
}

// Resend pending/unacknowledged QoS 1 & 2 PUBLISHs after timeout.
func (q *QoS12) MonitorTimeouts(ctx context.Context, toMS uint64, d func(*Item) error, wg *sync.WaitGroup) {
	q.queue.monitorTimeouts(ctx, toMS, d)
	wg.Done()
}

// Resend pending/unacknowledged QoS 2 PUBRELs after timeout.
func (q *QoS2Part2) MonitorTimeouts(ctx context.Context, toMS uint64, d func(*Item) error, wg *sync.WaitGroup) {
	q.queue.monitorTimeouts(ctx, toMS, d)
	wg.Done()
}

// Resend all pending QoS 1 & 2 PUBLISHs for new session.
func (q *QoS12) ResendAll(d func(*Item) error) error {
	q.Lock()
	for i := q.h; i != nil; i = i.next {
		if i.PId == 0 {
			break
		}
		if err := d(i); err != nil {
			q.Unlock()
			return err
		}

		q.toSend = i.next
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

	}
	q.Unlock()
	return nil
}
