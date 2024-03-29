package queue

import (
	"context"
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

func (q *Basic) Init() {
	q.trig = sync.NewCond(q)
}

func (q *Basic) Add(i *Item) {
	q.Lock()
	if q.h == nil {
		q.h = i
	} else {
		q.t.next = i // add singly
	}
	q.t = i
	q.Unlock()
	q.NotifyDispatcher()
}

// NotifyDispatcher will signal dispatcher to check the queue.
func (q *Basic) NotifyDispatcher() {
	q.trig.Signal()
}

// StartDispatcher will continuously dispatch queue items and remove them.
func (q *Basic) StartDispatcher(ctx context.Context, d func(*Item) error, wg *sync.WaitGroup) {
	defer func() {
		if wg != nil {
			wg.Done()
		}
	}()
	for {
		if ctx.Err() != nil {
			return
		}

		q.Lock()

		if q.h == nil {
			q.trig.Wait()
		}

		i := q.h
		if i != nil {
			q.h = i.next // remove singly
			if q.h == nil {
				q.t = nil
			}
		}

		q.Unlock()

		if i != nil {
			i.next = nil
			if err := d(i); err != nil {
				return
			}
			ReturnItem(i)
		}
	}
}

func (q *queue) Reset() {
	q.Lock()
	for i := q.h; i != nil; i = i.next {
		q.removeDoubly(i)
		i.P.FreeIfLastUser()
		ReturnItem(i)
	}
	q.h, q.t = nil, nil
	q.Unlock()
}

func (q *queue) addDoubly(i *Item) {
	if q.h == nil {
		q.h = i
	} else {
		q.t.next = i
		i.prev = q.t
	}
	q.t = i
}

func (q *queue) removeDoubly(i *Item) {
	if i == q.h {
		q.h = i.next
	} else {
		i.prev.next = i.next
	}

	if i == q.t {
		q.t = i.prev
	} else {
		i.next.prev = i.prev
	}

	i.prev, i.next = nil, nil // avoid memory leaks
}

// Resend pending/unacknowledged QoS 1 & 2 messages after timeout.
func (q *queue) monitorTimeouts(ctx context.Context, toS int64, timedOut func(*Item) error) {
	timeout := time.Second * time.Duration(toS)
	t := time.NewTimer(timeout)
	defer func() {
		if !t.Stop() {
			<-t.C
		}
	}()

	done := ctx.Done()
	for {
		select {
		case <-done:
			return
		case now := <-t.C:
			nextRun := timeout

			q.Lock()
			for i := q.h; i != nil; i = i.next {
				sent := atomic.LoadInt64(&i.Sent)
				if sent == 0 {
					break // if we encounter unsent, stop
				}
				if pending := now.Sub(time.Unix(sent, 0)); pending < timeout {
					if candidate := timeout - pending; candidate < nextRun {
						nextRun = candidate
					}
					continue
				}

				if err := timedOut(i); err != nil {
					q.Unlock()
					return
				}

			}
			q.Unlock()

			t.Reset(nextRun)
		}
	}
}
