package gobroke

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/RoanBrand/gobroke/internal/model"
	"github.com/RoanBrand/gobroke/internal/queue"
	log "github.com/sirupsen/logrus"
)

type session struct {
	client *client
	conn   net.Conn

	packet  packet
	rxState uint8

	ctx      context.Context
	cancel   context.CancelFunc
	onlyOnce sync.Once
	stopped  sync.WaitGroup

	clientId     string
	assignedCId  bool
	connectSent  bool
	connectFlags byte
	keepAlive    time.Duration
	protoVersion uint8

	will     *model.PubMessage
	userName string
	password []byte
}

type packet struct {
	controlType     uint8
	flags           uint8
	remainingLength uint32 // max 268,435,455 (256 MB)
	lenMul          uint32

	// Variable header
	vhToRead      uint32
	vhPropertyLen uint32
	vhBuf         []byte
	pID           uint16 // subscribe, unsubscribe, publish with QoS>0.

	payload []byte
}

func (s *session) run() {
	c := s.client

	if err := c.q2Stage2.ResendAll(s.sendPubrelQ); err != nil {
		log.WithFields(log.Fields{
			"ClientId": s.clientId,
			"err":      err,
		}).Error("Unable to resend all pending QoS2 PUBRELs")
	}
	if err := c.q2.ResendAll(s.sendPublish); err != nil {
		log.WithFields(log.Fields{
			"ClientId": s.clientId,
			"err":      err,
		}).Error("Unable to resend all pending QoS2 PUBLISHs")
	}
	if err := c.q1.ResendAll(s.sendPublish); err != nil {
		log.WithFields(log.Fields{
			"ClientId": s.clientId,
			"err":      err,
		}).Error("Unable to resend all pending QoS1 PUBLISHs")
	}

	s.stopped.Add(3)
	go c.q0.StartDispatcher(s.ctx, s.sendPublish, &s.stopped)
	go c.q1.StartDispatcher(s.ctx, s.sendPublish, s.getPId, &s.stopped)
	go c.q2.StartDispatcher(s.ctx, s.sendPublish, s.getPId, &s.stopped)

	// QoS 1&2 unacknowledged message resend timeout in ms
	// Set to 0 to disable. Will always resend once on new conn.
	// TODO: move to config
	var retryInterval uint64 = 50000

	if s.protoVersion != 5 && retryInterval > 0 {
		s.stopped.Add(3)
		go c.q2Stage2.MonitorTimeouts(s.ctx, retryInterval, s.sendPubrelQ, &s.stopped)
		go c.q1.MonitorTimeouts(s.ctx, retryInterval, s.sendPublish, &s.stopped)
		go c.q2.MonitorTimeouts(s.ctx, retryInterval, s.sendPublish, &s.stopped)
	}
}

func (s *session) stop() {
	s.onlyOnce.Do(func() {
		s.cancel()
		s.conn.Close()
		if c := s.client; c != nil {
			c.notifyFlusher()
			c.q0.NotifyDispatcher()
			c.q1.NotifyDispatcher()
			c.q2.NotifyDispatcher()
		}
		s.stopped.Wait()
	})
}

func (s *session) handlePubrec() error {
	pId := binary.BigEndian.Uint16(s.packet.vhBuf)
	dupV3 := !s.client.qos2Part1Done(pId) && s.protoVersion == 3 && s.client.q2Stage2.Present(pId)
	return s.sendPubrel(pId, dupV3)
}

func (s *session) handlePubrel() error {
	pId := binary.BigEndian.Uint16(s.packet.vhBuf)
	delete(s.client.q2RxLookup, pId)
	return s.sendPubcomp(pId)
}

func (s *session) getPId() uint16 {
	return <-s.client.pIDs
}

func (s *session) updateTimeout() {
	if s.keepAlive > 0 {
		s.conn.SetReadDeadline(time.Now().Add(s.keepAlive))
	}
}

func (s *session) sendPublish(i *queue.Item) error {
	var publish byte = model.PUBLISH

	if i.Retained {
		publish |= 0x01
	}

	rl := len(i.P.Pub) - 1
	qos := i.TxQoS
	if qos > 0 {
		rl += 2
		publish |= qos << 1

		if !i.Sent.IsZero() {
			publish |= 0x08 // set DUP if sent before
		}
		i.Sent = time.Now()
	}

	if s.protoVersion == 5 {
		rl++
	}

	s.client.txLock.Lock()

	s.client.tx.WriteByte(publish)

	model.VariableLengthEncodeNoAlloc(rl, func(eb byte) error {
		return s.client.tx.WriteByte(eb)
	})

	tLen := binary.BigEndian.Uint16(i.P.Pub[1:])
	// Topic Name
	s.client.tx.Write(i.P.Pub[1 : 3+tLen])

	// PId
	if qos > 0 {
		s.client.tx.WriteByte(byte(i.PId >> 8))
		s.client.tx.WriteByte(byte(i.PId))
	}

	// Properties
	if s.protoVersion == 5 {
		// TODO: support outgoing properties
		s.client.tx.WriteByte(0)
	}

	// Payload
	_, err := s.client.tx.Write(i.P.Pub[3+tLen:])

	s.client.txLock.Unlock()
	s.client.notifyFlusher()

	if qos == 0 {
		i.P.FreeIfLastUser()
	}

	return err
}

func (s *session) sendPuback(pId uint16) error {
	var rl uint8 = 2
	/*if s.protoVersion == 5 {
		rl += 2
	}*/

	s.client.txLock.Lock()

	s.client.tx.WriteByte(model.PUBACK)
	s.client.tx.WriteByte(rl)
	s.client.tx.WriteByte(byte(pId >> 8))
	err := s.client.tx.WriteByte(byte(pId))

	/*if s.protoVersion == 5 {
		s.client.tx.WriteByte(0)       // Reason Code
		err = s.client.tx.WriteByte(0) // Property Length
	}*/

	s.client.txLock.Unlock()
	s.client.notifyFlusher()
	return err
}

func (s *session) sendPubrec(pId uint16) error {
	var rl uint8 = 2
	/*if s.protoVersion == 5 {
		rl += 2
	}*/

	s.client.txLock.Lock()

	s.client.tx.WriteByte(model.PUBREC)
	s.client.tx.WriteByte(rl)
	s.client.tx.WriteByte(byte(pId >> 8))
	err := s.client.tx.WriteByte(byte(pId))

	/*if s.protoVersion == 5 {
		s.client.tx.WriteByte(0)       // Reason Code
		err = s.client.tx.WriteByte(0) // Property Length
	}*/

	s.client.txLock.Unlock()
	s.client.notifyFlusher()
	return err
}

func (s *session) sendPubrel(pId uint16, dupV3 bool) error {
	var header byte = model.PUBREL | 0x02
	var rl uint8 = 2
	/*if s.protoVersion == 5 {
		rl += 2
	} else */
	if dupV3 {
		header |= 0x08 // set mqtt3 DUP
	}

	s.client.txLock.Lock()

	s.client.tx.WriteByte(header)
	s.client.tx.WriteByte(rl)
	s.client.tx.WriteByte(byte(pId >> 8))
	err := s.client.tx.WriteByte(byte(pId))

	/*if s.protoVersion == 5 {
		s.client.tx.WriteByte(0)       // Reason Code
		err = s.client.tx.WriteByte(0) // Property Length
	}*/

	s.client.txLock.Unlock()
	s.client.notifyFlusher()
	return err
}

func (s *session) sendPubrelQ(i *queue.Item) error {
	err := s.sendPubrel(i.PId, s.protoVersion == 3 && !i.Sent.IsZero())
	i.Sent = time.Now()
	return err
}

func (s *session) sendPubcomp(pId uint16) error {
	var rl uint8 = 2
	/*if s.protoVersion == 5 {
		rl += 2
	}*/

	s.client.txLock.Lock()

	s.client.tx.WriteByte(model.PUBCOMP)
	s.client.tx.WriteByte(rl)
	s.client.tx.WriteByte(byte(pId >> 8))
	err := s.client.tx.WriteByte(byte(pId))

	/*if s.protoVersion == 5 {
		s.client.tx.WriteByte(0)       // Reason Code
		err = s.client.tx.WriteByte(0) // Property Length
	}*/

	s.client.txLock.Unlock()
	s.client.notifyFlusher()
	return err
}

func (s *session) sendSuback(reasonCodes []uint8) error {
	rl := len(reasonCodes) + 2
	if s.protoVersion == 5 {
		rl++
	}

	s.client.txLock.Lock()

	s.client.tx.WriteByte(model.SUBACK)

	model.VariableLengthEncodeNoAlloc(rl, func(eb byte) error {
		return s.client.tx.WriteByte(eb)
	})

	// Variable Header
	s.client.tx.WriteByte(s.packet.vhBuf[0]) // [MQTT-3.8.4-2]
	s.client.tx.WriteByte(s.packet.vhBuf[1])

	if s.protoVersion == 5 {
		s.client.tx.WriteByte(0) // TODO: support properties
	}

	// Payload
	_, err := s.client.tx.Write(reasonCodes) // [MQTT-3.9.3-1]

	s.client.txLock.Unlock()
	s.client.notifyFlusher()
	return err
}

func (s *session) sendUnsuback(nTopics int) error {
	rl := 2
	if s.protoVersion == 5 {
		rl += nTopics + 1
	}

	s.client.txLock.Lock()

	s.client.tx.WriteByte(model.UNSUBACK)

	model.VariableLengthEncodeNoAlloc(rl, func(eb byte) error {
		return s.client.tx.WriteByte(eb)
	})

	// Variable Header
	s.client.tx.WriteByte(s.packet.vhBuf[0])
	err := s.client.tx.WriteByte(s.packet.vhBuf[1])

	if s.protoVersion == 5 {
		s.client.tx.WriteByte(0) // TODO: support properties

		// Payload
		for i := 0; i < nTopics; i++ {
			err = s.client.tx.WriteByte(0) // TODO: support reason codes
		}

	}

	s.client.txLock.Unlock()
	s.client.notifyFlusher()
	return err
}

// Send CONNACK with optional Session Present flag.
func (s *session) sendConnack(errCode uint8, SP bool) error {
	p := []byte{model.CONNACK, 2, 0, errCode}
	if SP {
		p[2] = 1
	}
	_, err := s.conn.Write(p)
	return err
}

func (s *session) sendConnack5(reasonCode uint8, sessionPresent bool) error {
	props := []byte{
		41, 0, // Sub Ids not supported yet
		42, 0, // Shared subs not supported yet
	}
	if s.assignedCId {
		props = append(props, 18, 0, 0)
		binary.BigEndian.PutUint16(props[len(props)-2:], uint16(len(s.clientId)))
		props = append(props, []byte(s.clientId)...)
	}

	propsVarLen := make([]byte, 0, 4)
	propsVarLen = model.VariableLengthEncode(propsVarLen, len(props))

	vh := []byte{0, reasonCode}
	if sessionPresent {
		vh[0] = 1
	}

	p := make([]byte, 1, 16)
	p[0] = model.CONNACK
	p = model.VariableLengthEncode(p, 2+len(propsVarLen)+len(props))
	p = append(p, vh...)
	p = append(p, propsVarLen...)
	p = append(p, props...)

	return s.writePacket(p)
}

func (s *session) persistent() bool {
	return s.connectFlags&0x02 == 0
}

func (s *session) writePacket(p []byte) error {
	s.client.txLock.Lock()
	if _, err := s.client.tx.Write(p); err != nil {
		s.client.txLock.Unlock()
		return err
	}

	s.client.notifyFlusher()
	s.client.txLock.Unlock()
	return nil
}

func (s *Server) startSession(conn net.Conn) {
	ctx, cancel := context.WithCancel(s.ctx)
	conn.SetReadDeadline(time.Now().Add(time.Second * 10)) // CONNECT packet timeout
	ns := session{ctx: ctx, cancel: cancel, conn: conn}
	ns.packet.vhBuf, ns.packet.payload = make([]byte, 0, 512), make([]byte, 0, 512)

	ns.stopped.Add(1)
	graceFullExit := false
	defer func() {
		ns.stopped.Done()
		ns.stop()
		if ns.connectSent {
			if !ns.persistent() { // [MQTT-3.1.2-6]
				s.removeSession(&ns)
			}

			if !graceFullExit && ns.will != nil {
				s.pubs.Add(queue.GetItem(ns.will))
			}
			ns.will = nil
		}
	}()

	rx := make([]byte, 1024)

	for {
		nRx, err := conn.Read(rx)
		if err != nil {
			ns.readError(err)
			return
		}

		if nRx == 0 {
			continue
		}

		// [MQTT-3.1.0-1]
		if rx[0]&0xF0 != model.CONNECT {
			log.Debug("first packet from new connection is not CONNECT")
			return
		}

		if err = s.parseStream(&ns, rx[:nRx]); err != nil {
			if err == errCleanExit {
				graceFullExit = true
			} else {
				log.WithFields(log.Fields{
					"ClientId": ns.clientId,
					"err":      err,
				}).Debug("client failure")
			}
			return
		}

		break
	}

	for {
		nRx, err := conn.Read(rx)
		if err != nil {
			ns.readError(err)
			return
		}

		if err = s.parseStream(&ns, rx[:nRx]); err != nil {
			if err == errCleanExit {
				graceFullExit = true
			} else {
				log.WithFields(log.Fields{
					"ClientId": ns.clientId,
					"err":      err,
				}).Debug("client failure")
			}
			return
		}
	}
}

func (s *session) readError(err error) {
	if err.Error() == "EOF" || errors.Is(err, net.ErrClosed) {
		return
	}

	// KeepAlive timeout
	if strings.Contains(err.Error(), "i/o timeout") {
		l := log.WithFields(log.Fields{
			"ClientId": s.clientId,
		})
		if s.connectSent {
			l.Debug("KeepAlive timeout. Dropping connection")
		} else {
			l.Debug("Timeout waiting for CONNECT. Dropping connection")
		}
		return
	}

	log.WithFields(log.Fields{
		"ClientId": s.clientId,
		"err":      err,
	}).Error("TCP RX error")
}

func (s *session) startWriter() {
	defer s.stopped.Done()
	done := s.ctx.Done()

	for {
		select {
		case <-done:
			return
		case <-s.client.txFlush:
			s.client.txLock.Lock()
			if s.client.tx.Buffered() > 0 {
				if err := s.client.tx.Flush(); err != nil {
					s.client.txLock.Unlock()
					if errors.Is(err, net.ErrClosed) {
						return
					}

					log.WithFields(log.Fields{
						"ClientId": s.clientId,
						"err":      err,
					}).Error("TCP TX error")
					return
				}
			}
			s.client.txLock.Unlock()
		}
	}
}
