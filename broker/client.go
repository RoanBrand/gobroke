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
	q0Trig *sync.Cond
	q0Lock sync.Mutex

	// QoS 1
	q1Q      *list.List
	q1Lookup map[uint16]*list.Element
	q1Trig   *sync.Cond
	q1Lock   sync.Mutex

	// QoS 2
	q2RxLookup map[uint16]struct{}

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
		q1Lookup:      make(map[uint16]*list.Element, 2),
		q2RxLookup:    make(map[uint16]struct{}, 2),
		clear:         make(chan struct{}),
	}
	c.q0Trig = sync.NewCond(&c.q0Lock)
	c.q1Trig = sync.NewCond(&c.q1Lock)

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
	for i := range c.q1Lookup {
		delete(c.q1Lookup, i)
	}
	c.q1Lock.Unlock()

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
		var pubP []byte
		if sp.retained {
			pubP = make([]byte, len(sp.p.pacs[0]))
			pubP[0] |= 0x01
		} else {
			pubP = sp.p.pacs[0]
		}

		c.q0Lock.Lock()
		c.q0Q.PushBack(pubP)
		c.q0Trig.Signal()
		c.q0Lock.Unlock()
	case 1:
		pubP := make([]byte, len(sp.p.pacs[1]))
		copy(pubP, sp.p.pacs[1])
		c.publishId++
		pubP[sp.p.idLoc] = uint8(c.publishId >> 8)
		pubP[sp.p.idLoc+1] = uint8(c.publishId)
		if sp.retained {
			pubP[0] |= 0x01
		}

		c.q1Lock.Lock()
		c.q1Lookup[c.publishId] = c.q1Q.PushBack(&qosPub{done: make(chan struct{}), p: pubP})
		c.q1Trig.Signal()
		c.q1Lock.Unlock()
	}
}

func (c *client) qos1Done(pID uint16) {
	c.q1Lock.Lock()
	if qPub, ok := c.q1Lookup[pID]; ok {
		close(c.q1Q.Remove(qPub).(*qosPub).done)
		delete(c.q1Lookup, pID)
		c.q1Lock.Unlock()
	} else {
		c.q1Lock.Unlock()
		log.WithFields(log.Fields{
			"client":   c.session.clientId,
			"packetID": pID,
		}).Error("Got PUBACK packet for none existing packet")
	}
}

type topL struct {
	children topT
}

type topT map[string]*topL
