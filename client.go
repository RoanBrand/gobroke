package gobroke

import (
	"bufio"
	"sync"
	"time"

	"github.com/RoanBrand/gobroke/internal/model"
	"github.com/RoanBrand/gobroke/internal/queue"
	log "github.com/sirupsen/logrus"
)

type client struct {
	session       *session
	subscriptions topT
	pIDs          chan uint16 // [MQTT-2.3.1-4]
	acks          []byte

	tx      *bufio.Writer
	txFlush chan struct{}
	txLock  sync.Mutex

	// Queued outbound messages
	q0         queue.Basic
	q1         queue.QoS12
	q2         queue.QoS12
	q2Stage2   queue.QoS2Part2
	q2RxLookup map[uint16]struct{} // inbound
}

func newClient(ses *session) *client {
	c := client{
		session:       ses,
		subscriptions: make(topT),
		pIDs:          make(chan uint16, 65535),
		acks:          make([]byte, 4),
		tx:            bufio.NewWriter(ses.conn),
		txFlush:       make(chan struct{}, 1),
		q2RxLookup:    make(map[uint16]struct{}, 2),
	}

	c.q0.Init()
	c.q1.Init()
	c.q2.Init()
	c.q2Stage2.Init()

	for i := 1; i <= 65535; i++ { // some clients don't like 0
		c.pIDs <- uint16(i)
	}

	return &c
}

func (c *client) clearState() {
	c.q0.Reset()
	c.q1.Reset()
	c.q2.Reset()
	c.q2Stage2.Reset()

	for len(c.pIDs) > 0 {
		<-c.pIDs
	}
	for i := 1; i <= 65535; i++ {
		c.pIDs <- uint16(i)
	}
	for i := range c.q2RxLookup {
		delete(c.q2RxLookup, i)
	}
}

func (c *client) replaceSession(s *session) {
	c.txLock.Lock()
	c.tx.Reset(s.conn)
	c.txLock.Unlock()
	c.session = s
}

func (c *client) notifyFlusher() {
	if len(c.txFlush) == 0 {
		select {
		case c.txFlush <- struct{}{}:
		default:
		}
	}
}

func (c *client) processPub(p *model.PubMessage, maxQoS uint8, retained bool) {
	finalQoS := p.RxQoS()
	if maxQoS < finalQoS {
		finalQoS = maxQoS
	}

	i := queue.GetItem(p)
	i.TxQoS, i.Retained = finalQoS, retained
	p.AddUser()

	switch finalQoS {
	case 0:
		c.q0.Add(i)
	case 1:
		c.q1.Add(i)
	case 2:
		c.q2.Add(i)
	}
}

func (c *client) qos1Done(pID uint16) {
	i := c.q1.Remove(pID)
	if i == nil {
		log.WithFields(log.Fields{
			"clientId": c.session.clientId,
			"packetID": pID,
		}).Error("Got PUBACK packet for none existing packet")
		return
	}

	i.P.FreeIfLastUser()
	queue.ReturnItem(i)
	c.pIDs <- pID
}

func (c *client) qos2Part1Done(pID uint16) bool {
	i := c.q2.Remove(pID)
	if i == nil {
		log.WithFields(log.Fields{
			"clientId": c.session.clientId,
			"packetID": pID,
		}).Error("Got PUBREC packet for none existing packet")
		return false
	}

	i.P.FreeIfLastUser()
	i.Sent = time.Now()
	c.q2Stage2.Add(i)
	return true
}

func (c *client) qos2Part2Done(pID uint16) {
	i := c.q2Stage2.Remove(pID)
	if i == nil {
		log.WithFields(log.Fields{
			"clientId": c.session.clientId,
			"packetID": pID,
		}).Error("Got PUBCOMP packet for none existing packet")
		return
	}

	queue.ReturnItem(i)
	c.pIDs <- pID
}

type topL struct {
	subscribed bool // to this exact level
	children   topT
}

type topT map[string]*topL
