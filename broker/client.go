package broker

import (
	"bufio"
	"container/list"
	"sync"

	log "github.com/sirupsen/logrus"
)

type client struct {
	session       *session
	subscriptions topT

	tx      *bufio.Writer
	txFlush chan struct{}
	txLock  sync.Mutex

	pubRX     chan subPub // from server
	publishId uint16

	// QoS 0
	q0Q    *list.List
	q0Cond *sync.Cond
	q0Lock sync.Mutex

	// QoS 1
	q1Q     *list.List
	qLookup map[uint16]*list.Element
	q1Cond  *sync.Cond
	q1Lock  sync.Mutex

	clear chan struct{}
}

func newClient(ses *session) *client {
	c := client{
		session:       ses,
		subscriptions: make(topT, 4),
		tx:            bufio.NewWriter(ses.conn),
		txFlush:       make(chan struct{}, 1),
		pubRX:         make(chan subPub, 1),
		q0Q:           list.New(),
		q1Q:           list.New(),
		qLookup:       make(map[uint16]*list.Element, 2),
		clear:         make(chan struct{}),
	}
	c.q0Cond = sync.NewCond(&c.q0Lock)
	c.q1Cond = sync.NewCond(&c.q1Lock)

	go c.run()
	return &c
}

func (c *client) run() {
	for {
		select {
		case p := <-c.pubRX:
			c.processPub(p)
		case <-c.clear:
			c.clearState()
			c.clear <- struct{}{}
		}
	}
}

func (c *client) clearState() {
	c.publishId = 0

	c.q0Lock.Lock()
	c.q0Q.Init()
	c.q0Lock.Unlock()

	c.q1Lock.Lock()
	c.q1Q.Init()
	for i := range c.qLookup {
		delete(c.qLookup, i)
	}
	c.q1Lock.Unlock()
}

func (c *client) replaceSession(s *session) {
	c.txLock.Lock()
	c.tx.Reset(s.conn)
	c.txLock.Unlock()
	c.session = s
}

type qosPub struct {
	done chan struct{}
	p    []byte
	sent bool
}

func (c *client) processPub(sp subPub) {
	finalQoS := sp.p.pubQoS
	if sp.maxQoS < sp.p.pubQoS {
		finalQoS = sp.maxQoS
	}

	switch finalQoS {
	case 0:
		c.q0Lock.Lock()
		c.q0Q.PushBack(sp.p.pacs[0])
		c.q0Cond.Signal()
		c.q0Lock.Unlock()
	case 1:
		pubP := make([]byte, len(sp.p.pacs[1]))
		copy(pubP, sp.p.pacs[1])
		c.publishId++
		pubP[sp.p.idLoc] = uint8(c.publishId >> 8)
		pubP[sp.p.idLoc+1] = uint8(c.publishId)

		c.q1Lock.Lock()
		c.qLookup[c.publishId] = c.q1Q.PushBack(&qosPub{done: make(chan struct{}), p: pubP})
		c.q1Cond.Signal()
		c.q1Lock.Unlock()
	}
}

func (c *client) qos1Done(pId uint16) {
	c.q1Lock.Lock()
	qPub, ok := c.qLookup[pId]
	if ok {
		close(c.q1Q.Remove(qPub).(*qosPub).done)
		delete(c.qLookup, pId)
		c.q1Lock.Unlock()
	} else {
		c.q1Lock.Unlock()
		log.WithFields(log.Fields{
			"client":   c.session.clientId,
			"packetID": pId,
		}).Error("Got PUBACK packet for none existing packet")
	}
}

type topL struct {
	children topT
}

type topT map[string]*topL
