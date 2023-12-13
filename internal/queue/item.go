package queue

import (
	"sync"

	"github.com/RoanBrand/gobroke/internal/model"
)

// Item is stored in various publish message queues.
type Item struct {
	Sent int64 // QoS 1&2, PUBREL

	P *model.PubMessage

	SId      uint32 // Subscription Id. PUBLISH from Server to Client.
	PId      uint16 // QoS 1&2, PUBREL
	TxQoS    uint8
	Retained bool // Msg sent to client due to subscription

	DbMsgTxId uint64

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
