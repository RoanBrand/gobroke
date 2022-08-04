package gobroke

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"github.com/RoanBrand/gobroke/internal/model"
	"github.com/RoanBrand/gobroke/internal/queue"
	log "github.com/sirupsen/logrus"
)

var aLongTimeAgo = time.Unix(1, 0) // used for cancellation

type session struct {
	client *client
	conn   net.Conn

	packet  packet
	rxState uint8

	onlyOnce             sync.Once
	ctx                  context.Context
	cancel               context.CancelFunc
	ended                sync.WaitGroup
	disconnectReasonCode uint8

	clientId       string
	assignedCId    bool
	connectSent    bool
	connectFlags   byte
	protoVersion   uint8
	keepAlive      time.Duration
	expiryInterval uint32

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
	vhToRead     uint32
	vhPropToRead uint32
	vhBuf        []byte
	pID          uint16 // subscribe, unsubscribe, publish with QoS>0.

	payload []byte // sometimes used as temp buf for tx from reader thread
}

func (s *session) run() {
	c := s.client

	if err := c.q2Stage2.ResendAll(s.sendPubrelFromQueue); err != nil {
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

	s.ended.Add(3)
	go c.q0.StartDispatcher(s.ctx, s.sendPublish, &s.ended)
	go c.q1.StartDispatcher(s.ctx, s.sendPublish, s.getPId, &s.ended)
	go c.q2.StartDispatcher(s.ctx, s.sendPublish, s.getPId, &s.ended)

	// QoS 1&2 unacknowledged message resend timeout in ms
	// Set to 0 to disable. Will always resend once on new conn.
	// TODO: move to config
	var retryInterval uint64 = 50000

	if s.protoVersion != 5 && retryInterval > 0 {
		s.ended.Add(3)
		go c.q2Stage2.MonitorTimeouts(s.ctx, retryInterval, s.sendPubrelFromQueue, &s.ended)
		go c.q1.MonitorTimeouts(s.ctx, retryInterval, s.sendPublish, &s.ended)
		go c.q2.MonitorTimeouts(s.ctx, retryInterval, s.sendPublish, &s.ended)
	}
}

func (s *session) end(disconnectRC uint8) {
	s.onlyOnce.Do(func() {
		s.conn.SetReadDeadline(aLongTimeAgo)
		s.cancel()

		c := s.client
		if c != nil {
			c.q0.NotifyDispatcher()
			c.q1.NotifyDispatcher()
			c.q2.NotifyDispatcher()
		}

		s.ended.Wait()
		var err error

		if c != nil {
			c.txLock.Lock()
			err = c.tx.Flush()
			c.tx.Reset(nil)
			c.txLock.Unlock()
		}
		if err != nil {
			log.WithFields(log.Fields{
				"ClientId": s.clientId,
				"err":      err,
			}).Error("failed to flush tx buffer")
		}

		if err = s.sendDisconnect(disconnectRC); err != nil {
			log.WithFields(log.Fields{
				"ClientId": s.clientId,
				"err":      err,
			}).Error("failed to send DISCONNECT")
		}

		s.conn.Close()
		s.conn = nil
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
	return s.sendPubcomp()
}

func (s *session) getPId() uint16 {
	return <-s.client.pIDs
}

func (s *session) updateTimeout() {
	if s.keepAlive > 0 {
		s.conn.SetReadDeadline(time.Now().Add(s.keepAlive))
	}
}

var connackProps = []byte{
	41, 0, // Sub Ids not supported yet
	42, 0, // Shared subs not supported yet
}

// Send CONNACK with success 0.
func (s *session) sendConnackSuccess(sessionPresent bool) error {
	p := s.packet.payload[:0]
	p = append(p, model.CONNACK)

	if s.protoVersion < 5 {
		p = append(p, 2)
		if sessionPresent {
			p = append(p, 1)
		} else {
			p = append(p, 0)
		}
		p = append(p, 0) // success
		return s.writePacket(p)
	}

	propsLen := len(connackProps)
	if s.assignedCId {
		propsLen += 3 + len(s.clientId)
	}

	rl := 2 + model.LengthToNumberOfVariableLengthBytes(propsLen) + propsLen

	p = model.VariableLengthEncode(p, rl)

	// Ack Flags
	if sessionPresent {
		p = append(p, 1)
	} else {
		p = append(p, 0)
	}

	// Reason Code
	p = append(p, 0) // success

	p = model.VariableLengthEncode(p, propsLen)
	p = append(p, connackProps...)

	if s.assignedCId {
		cIdLen := uint16(len(s.clientId))
		p = append(p, 18, byte(cIdLen>>8), byte(cIdLen))
		p = append(p, []byte(s.clientId)...)
	}

	return s.writePacket(p)
}

// direct write to conn as writer not started yet
func (s *session) sendConnackFail(reasonCode uint8) error {
	// use packet.vhBuf as buffer, as won't be used anymore
	p := s.packet.vhBuf
	p[0], p[2], p[3] = model.CONNACK, 0, reasonCode

	if s.protoVersion < 5 {
		p[1] = 2
		_, err := s.conn.Write(p[:4])
		return err
	}

	p[1], p[4] = 3, 0
	_, err := s.conn.Write(p[:5])
	return err
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
	// TODO: support Reason Code, Properties
	p := s.packet.payload[:0]
	p = append(p, model.PUBACK, 2, byte(pId>>8), byte(pId))
	return s.writePacket(p)
}

func (s *session) sendPubrec(pId uint16) error {
	// TODO: support Reason Code, Properties
	p := s.packet.payload[:0]
	p = append(p, model.PUBREC, 2, byte(pId>>8), byte(pId))
	return s.writePacket(p)
}

func (s *session) sendPubrel(pId uint16, dupV3 bool) error {
	// TODO: support Reason Code, Properties
	p := s.packet.payload[:0]
	p = append(p, model.PUBRELSend, 2, byte(pId>>8), byte(pId))
	return s.writePacket(p)
}

func (s *session) sendPubrelFromQueue(i *queue.Item) error {
	var header byte = model.PUBRELSend
	if s.protoVersion == 3 && !i.Sent.IsZero() {
		header |= 0x08 // set mqtt3 DUP
	}

	s.client.txLock.Lock()

	s.client.tx.WriteByte(header)
	s.client.tx.WriteByte(2)
	s.client.tx.WriteByte(byte(i.PId >> 8))
	err := s.client.tx.WriteByte(byte(i.PId))

	s.client.txLock.Unlock()
	s.client.notifyFlusher()

	i.Sent = time.Now()
	return err
}

func (s *session) sendPubcomp() error {
	// TODO: support Reason Code, Properties
	p := s.packet.payload[:0]
	p = append(p, model.PUBCOMP, 2, s.packet.vhBuf[0], s.packet.vhBuf[1])
	return s.writePacket(p)
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

func (s *session) sendDisconnect(disconnectRC uint8) error {
	if s.protoVersion < 5 || disconnectRC == 0 {
		return nil
	}

	p := s.packet.payload[:0]
	p = append(p, model.DISCONNECT, 1, disconnectRC)

	_, err := s.conn.Write(p)
	return err
}

func (s *session) cleanStart() bool {
	return s.connectFlags&0x02 == 2
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

	var err error
	ns.ended.Add(1)

	defer func() {
		ns.ended.Done()
		ns.end(ns.disconnectReasonCode)
		if !ns.connectSent {
			return
		}

		if ns.protoVersion < 5 {
			if ns.cleanStart() { // CleanSession
				s.removeSession(&ns) // v4[MQTT-3.1.2-6]
			}
		} else if ns.expiryInterval == 0 {
			s.removeSession(&ns)
		} else if ns.expiryInterval < 0xFFFFFFFF {
			expireTime := time.Second * time.Duration(ns.expiryInterval)
			// TODO: remove 10 year limit once persistence available
			if expireTime < time.Hour*24*365*10 {
				time.AfterFunc(expireTime, func() {
					s.removeSession(&ns)
				})
			}
		}

		if ns.will != nil {
			if err != errGotDisconnect || ns.disconnectReasonCode != 0 {
				s.pubs.Add(queue.GetItem(ns.will))
			}
			ns.will = nil
		}
	}()

	rx := make([]byte, 1024)
	var nRx int

	for {
		nRx, err = conn.Read(rx)
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
			if err != errGotDisconnect {
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
		nRx, err = conn.Read(rx)
		if err != nil {
			ns.readError(err)
			return
		}

		if err = s.parseStream(&ns, rx[:nRx]); err != nil {
			if err != errGotDisconnect {
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

	if errors.Is(err, os.ErrDeadlineExceeded) {
		if s.ctx.Err() != nil {
			return // because of session ended
		}

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
	defer s.ended.Done()
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
