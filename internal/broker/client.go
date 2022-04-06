package broker

import (
	"bufio"
	"sync"

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
		subscriptions: make(topT, 4),
		pIDs:          make(chan uint16, 65536),
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

func (c *client) processPub(p model.PubMessage, maxQoS uint8, retained bool) {
	finalQoS := p.RxQoS()
	if maxQoS < finalQoS {
		finalQoS = maxQoS
	}

	i := &queue.Item{P: p, TxQoS: finalQoS, Retained: retained}

	if finalQoS == 0 {
		c.q0.Add(i)
	} else {
		select {
		case pID := <-c.pIDs:
			i.PId = pID
			if finalQoS == 1 {
				c.q1.Add(i)
			} else {
				c.q2.Add(i)
			}
		default: // TODO: queue messages anyway and assign IDs later as they become available?
		}
	}
}

func (c *client) qos1Done(pID uint16) {
	if c.q1.Remove(pID) == nil {
		log.WithFields(log.Fields{
			"client":   c.session.clientId,
			"packetID": pID,
		}).Error("Got PUBACK packet for none existing packet")
		return
	}

	c.pIDs <- pID
}

func (c *client) qos2Part1Done(pID uint16) {
	i := c.q2.Remove(pID)
	if i == nil {
		log.WithFields(log.Fields{
			"client":   c.session.clientId,
			"packetID": pID,
		}).Error("Got PUBREC packet for none existing packet")
		return
	}

	c.q2Stage2.Add(i)
}

func (c *client) qos2Part2Done(pID uint16) {
	if c.q2Stage2.Remove(pID) == nil {
		log.WithFields(log.Fields{
			"client":   c.session.clientId,
			"packetID": pID,
		}).Error("Got PUBCOMP packet for none existing packet")
		return
	}

	c.pIDs <- pID
}

type topL struct {
	subscribed bool // to this exact level
	children   topT
}

type topT map[string]*topL
