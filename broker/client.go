package broker

import (
	"bufio"
	"sync"

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
	q0         queue.QoS0
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
	c.q2Stage2.Init([]byte{PUBREL | 0x02, 2, 0, 0})

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

func (c *client) processPub(p *pub, maxQoS uint8, retained bool) {
	finalQoS := p.pubQoS
	if maxQoS < p.pubQoS {
		finalQoS = maxQoS
	}

	if finalQoS == 0 {
		var pubP []byte
		if retained {
			// Make copy, otherwise we race with original packet being sent out without retain flag set.
			pubP = make([]byte, len(p.pacs[0]))
			copy(pubP, p.pacs[0])
			pubP[0] |= 0x01
		} else {
			pubP = p.pacs[0]
		}

		c.q0.Add(pubP)
	} else { // QoS 1 & 2
		select {
		case pID := <-c.pIDs:
			pubP := make([]byte, len(p.pacs[1]))
			copy(pubP, p.pacs[1])

			pubP[p.idLoc], pubP[p.idLoc+1] = uint8(pID>>8), uint8(pID)
			if retained {
				pubP[0] |= 0x01
			}

			pubP[0] |= finalQoS << 1
			if finalQoS == 1 {
				c.q1.Add(pID, pubP)
			} else {
				c.q2.Add(pID, pubP)
			}
		default: // TODO: queue messages anyway and assign IDs later as they become available?
		}
	}
}

func (c *client) qos1Done(pID uint16) {
	if !c.q1.Remove(pID) {
		log.WithFields(log.Fields{
			"client":   c.session.clientId,
			"packetID": pID,
		}).Error("Got PUBACK packet for none existing packet")
		return
	}
	c.pIDs <- pID
}

func (c *client) qos2Part1Done(pID uint16) {
	if !c.q2.Remove(pID) {
		log.WithFields(log.Fields{
			"client":   c.session.clientId,
			"packetID": pID,
		}).Error("Got PUBREC packet for none existing packet")
		return
	}
	c.q2Stage2.Add(pID)
}

func (c *client) qos2Part2Done(pID uint16) {
	if !c.q2Stage2.Remove(pID) {
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
