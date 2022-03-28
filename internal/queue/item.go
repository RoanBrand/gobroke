package queue

import (
	"time"

	"github.com/RoanBrand/gobroke/internal/model"
)

// Item is stored in various publish message queues.
type Item struct {
	P        model.PubMessage // PUBLISH
	TxQoS    uint8            // PUBLISH
	PId      uint16           // PUBLISH QoS 1&2, PUBREL
	Retained bool             // PUBLISH. Msg sent by server due to client subscription to topic with retained message

	Sent time.Time // PUBLISH QoS 1&2, PUBREL

	next, prev *Item
}
