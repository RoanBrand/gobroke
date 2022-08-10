package queue

import (
	"sync"

	"github.com/RoanBrand/gobroke/internal/model"
)

// Item is stored in various publish message queues.
type Item struct {
	P *model.PubMessage

	// up to 7Feb2106 with time.Now().Unix()
	Sent uint32 // QoS 1&2, PUBREL

	PId      uint16 // QoS 1&2, PUBREL
	TxQoS    uint8
	Retained bool // Msg sent to client due to subscription

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
	i.PId, i.Sent = 0, 0
	ReturnItem(i)
}
