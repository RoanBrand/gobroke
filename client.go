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
	server        *Server
	session       *session
	subscriptions topT
	pIDs          chan uint16 // [MQTT-2.3.1-4]

	tx      *bufio.Writer
	txFlush chan struct{}
	txLock  sync.Mutex

	// Queued outbound messages
	q0         queue.Basic
	q1         queue.QoS12
	q2         queue.QoS12
	q2Stage2   queue.QoS2Part2
	q2RxLookup map[uint16]struct{} // inbound

	dbClientId   uint64
	dbMsgRxIdCnt uint64
}

func (s *Server) newClient(ses *session) *client {
	c := client{
		server:        s,
		session:       ses,
		subscriptions: make(topT),
		pIDs:          make(chan uint16, 65535),
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

func (s *Server) processPub(c *client, p *model.PubMessage, sub *subscription, retained bool) {
	if noLocal(sub.options) && c.session.clientId == p.Publisher {
		return // v5[MQTT-3.8.3-3]
	}

	finalQoS := p.RxQoS()
	if m := maxQoS(sub.options); m < finalQoS {
		finalQoS = m
	}

	i := queue.GetItem(p)
	i.SId, i.TxQoS, i.Retained = sub.id, finalQoS, retained
	if p.ToRetain() && retainAsPublished(sub.options) {
		i.Retained = true
	}

	err := s.diskNewPublishedMsgToClient(c, i, retained)
	if err != nil {
		log.Error(err)
	}

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
	i := c.q1.RemoveId(pID)
	if i == nil {
		log.WithFields(log.Fields{
			"ClientId": c.session.clientId,
			"packetID": pID,
		}).Debug("PUBACK received with unknown pId")
		return
	}

	err := c.server.diskDeleteClientMsg(c.dbClientId, i.DbMsgTxId)
	if err != nil {
		log.Error(err)
	}

	pid := i.P.DbSPubId
	if i.P.FreeIfLastUser() {
		err := c.server.diskDeleteServerMsg(pid)
		if err != nil {
			log.Error(err)
		}
	}

	queue.ReturnItemQos12(i)
	c.pIDs <- pID
}

func (c *client) qos2Part1Done(pID uint16) bool {
	i := c.q2.RemoveId(pID)
	if i == nil {
		log.WithFields(log.Fields{
			"ClientId": c.session.clientId,
			"packetID": pID,
		}).Debug("PUBREC received with unknown pId")
		return false
	}

	err := c.server.diskDeleteClientMsg(c.dbClientId, i.DbMsgTxId)
	if err != nil {
		log.Error(err)
	}

	pid := i.P.DbSPubId
	if i.P.FreeIfLastUser() {
		err := c.server.diskDeleteServerMsg(pid)
		if err != nil {
			log.Error(err)
		}
	}

	i.P, i.Sent = nil, time.Now().Unix()
	c.q2Stage2.Add(i)
	return true
}

func (c *client) qos2Part2Done(pID uint16) {
	i := c.q2Stage2.Remove(pID)
	if i == nil {
		log.WithFields(log.Fields{
			"ClientId": c.session.clientId,
			"packetID": pID,
		}).Debug("PUBCOMP received with unknown pId")
		return
	}

	queue.ReturnItemQos12(i)
	c.pIDs <- pID
}

type topL struct {
	children     topT
	sharedGroups map[string]struct{}
	subscribed   bool // to this exact level
}

type topT map[string]*topL
