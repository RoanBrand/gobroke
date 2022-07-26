package queue

import (
	"sync"
	"time"

	"github.com/RoanBrand/gobroke/internal/model"
)

// Item is stored in various publish message queues.
type Item struct {
	P        *model.PubMessage // PUBLISH
	PId      uint16            // PUBLISH QoS 1&2, PUBREL
	TxQoS    uint8             // PUBLISH
	Retained bool              // PUBLISH. Msg sent by server due to client subscription to topic with retained message

	Sent time.Time // PUBLISH QoS 1&2, PUBREL

	next, prev *Item
}

var pool = sync.Pool{}

func GetItem(p *model.PubMessage) (i *Item) {
	if pi := pool.Get(); pi == nil {
		i = new(Item)
	} else {
		i = pi.(*Item)
	}

	i.P = p
	return i
}

func ReturnItem(i *Item) {
	i.P = nil
	pool.Put(i)
}

func ReturnItemQos12(i *Item) {
	i.PId, i.Sent = 0, time.Time{}
	ReturnItem(i)
}
