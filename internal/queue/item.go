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

var pool = sync.Pool{
	New: func() any {
		return new(Item)
	},
}

func GetItem(p *model.PubMessage) *Item {
	i := pool.Get().(*Item)
	i.P = p
	return i
}

func ReturnItem(i *Item) {
	i.P = nil
	i.PId = 0
	i.Sent = time.Time{}
	pool.Put(i)
}
