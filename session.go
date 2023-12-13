package gobroke

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoanBrand/gobroke/internal/config"
	"github.com/RoanBrand/gobroke/internal/model"
	"github.com/RoanBrand/gobroke/internal/queue"
	log "github.com/sirupsen/logrus"
)

var aLongTimeAgo = time.Unix(1, 0) // used for cancellation

type session struct {
	client   *client
	conn     net.Conn
	clientId string
	will     *model.PubMessage

	packet packet

	onlyOnce sync.Once
	ctx      context.Context
	cancel   context.CancelFunc
	ended    sync.WaitGroup

	taToClient   topicAliasesClient
	taFromClient map[uint16][]byte

	keepAlive            time.Duration
	sendQuota            chan struct{}
	expiryInterval       uint32
	maxPacketSize        uint32
	receiveMax           uint16
	topicAliasMax        uint16
	connectFlags         uint8
	disconnectReasonCode uint8
	protoVersion         uint8

	connectSent    bool
	assignedCId    bool
	reqRespInfo    bool
	reqProblemInfo bool
}

type packet struct {
	vhBuf   []byte
	payload []byte // sometimes used as temp buf for tx from reader thread

	remainingLength int // max 268,435,455 (256 MB)
	lenMul          int

	// Variable header
	vhToRead     int
	vhPropToRead int
	expiry       uint32

	pID        uint16 // subscribe, unsubscribe, publish with QoS>0.
	topicAlias uint16 // PUBLISH

	rxState     uint8
	controlType uint8
	flags       uint8
}

type topicAliasesClient struct {
	sync.RWMutex
	aliases map[string]uint16
	left    int32
}

func (s *session) run(timeoutQoS12MQTT34 int64) {
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

	if s.protoVersion < 5 && timeoutQoS12MQTT34 > 0 {
		s.ended.Add(3)
		go c.q2Stage2.MonitorTimeouts(s.ctx, timeoutQoS12MQTT34, s.sendPubrelFromQueue, &s.ended)
		go c.q1.MonitorTimeouts(s.ctx, timeoutQoS12MQTT34, s.sendPublish, &s.ended)
		go c.q2.MonitorTimeouts(s.ctx, timeoutQoS12MQTT34, s.sendPublish, &s.ended)
	}
}

func (s *session) end(disconnectRC uint8) {
	s.onlyOnce.Do(func() {
		if s.conn != nil {
			s.conn.SetReadDeadline(aLongTimeAgo)
		}

		if s.cancel != nil {
			s.cancel()
		}

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

		if s.conn != nil {
			s.conn.Close()
			s.conn = nil
		}
	})
}

func (s *session) checkSharedSubscriptionTopicFilter(t [][]byte) error {
	if len(t) < 3 || len(t[1]) == 0 { // v5[MQTT-4.8.2-1]
		s.disconnectReasonCode = model.ProtocolError
		return errors.New("bad Shared Subscription ShareName or Topic Filter")
	}
	if err := checkUTF8(t[1], true); err != nil { // TODO: just check wildcards as UTF8 already checked
		s.disconnectReasonCode = model.ProtocolError
		return errors.New("bad Shared Subscription ShareName: " + err.Error())
	}
	if len(t) == 3 && len(t[2]) == 0 { // v5[MQTT-4.8.2-2]
		s.disconnectReasonCode = model.ProtocolError
		return errors.New("bad Shared Subscription Topic Filter: must be at least one char")
	}

	return nil
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

func (s *session) handleDisconnect() error {
	switch s.packet.vhBuf[0] {
	case model.NormalDisconnection:
		log.WithFields(log.Fields{
			"ClientId": s.clientId,
			"Reason":   "Normal disconnection",
		}).Debug("DISCONNECT received")

		return errGotNormalDiscon
	case model.DisconnectWithWill:
		log.WithFields(log.Fields{
			"ClientId": s.clientId,
			"Reason":   "Disconnect with Will Msg",
		}).Debug("DISCONNECT received")

		return errGotDisconWithWill
	default:
		log.WithFields(log.Fields{
			"ClientId":    s.clientId,
			"Reason Code": s.packet.vhBuf[0],
		}).Debug("DISCONNECT received")

		return nil
	}
}

func (s *session) getPId() uint16 {
	s.decSendQuota()
	return <-s.client.pIDs
}

func (s *session) decSendQuota() {
	if s.sendQuota != nil {
		select {
		case <-s.sendQuota:
		case <-s.ctx.Done():
		}
	}
}

func (s *session) incSendQuota() {
	if s.sendQuota != nil {
		select {
		case s.sendQuota <- struct{}{}:
		default:
		}
	}
}

func (s *session) updateTimeout() {
	if s.keepAlive > 0 {
		s.conn.SetReadDeadline(time.Now().Add(s.keepAlive))
	}
}

var connackProps = []byte{
	model.TopicAliasMaximum, 0xFF, 0xFF, // support max value Topic Alias Maximum
}

// Send CONNACK with success 0.
func (s *session) sendConnackSuccess(c *config.Config, sessionPresent bool) error {
	p := s.packet.payload[:0]
	p = append(p, model.CONNACK)

	if s.protoVersion < 5 {
		s.keepAlive *= time.Second * 3 / 2 // v4[MQTT-3.1.2-24]

		p = append(p, 2)
		if sessionPresent {
			p = append(p, 1)
		} else {
			p = append(p, 0)
		}
		p = append(p, 0) // success
		return s.writePacket(p)
	}

	// Assigned Client Identifier
	propsLen := len(connackProps)
	if s.assignedCId {
		propsLen += 3 + len(s.clientId)
	}

	// Server Keep Alive
	var serverKA uint16
	if c.KeepAliveOverrideMQTT5 > 0 {
		s.keepAlive = time.Duration(c.KeepAliveOverrideMQTT5)
		serverKA = c.KeepAliveOverrideMQTT5
		propsLen += 3
	} else if c.KeepAliveMaxMQTT5 > 0 && s.keepAlive > time.Duration(c.KeepAliveMaxMQTT5) {
		s.keepAlive = time.Duration(c.KeepAliveMaxMQTT5)
		serverKA = c.KeepAliveMaxMQTT5
		propsLen += 3
	}

	s.keepAlive *= time.Second * 3 / 2 // v5[MQTT-3.1.2-22]

	// Response Information
	var rit []byte
	if s.reqRespInfo { // TODO: Do not include if will cause to exceed MaxPackSize
		rit = generateRandomID()
		propsLen += 6 + len(rit) // cmd+2UTF8len + "ri-"
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

	// Properties
	p = model.VariableLengthEncode(p, propsLen)
	p = append(p, connackProps...)

	if s.assignedCId {
		l := len(s.clientId)
		p = append(p, model.AssignedClientIdentifier, byte(l>>8), byte(l))
		p = append(p, []byte(s.clientId)...)
	}

	if s.reqRespInfo {
		l := len(rit) + 3
		p = append(p, model.ResponseInformation, byte(l>>8), byte(l), 'r', 'i', '-')
		p = append(p, rit...)
	}

	if serverKA > 0 {
		p = append(p, model.ServerKeepAlive, byte(serverKA>>8), byte(serverKA))
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
	var now int64

	if i.Retained {
		publish |= 0x01
	}

	rl := len(i.P.B) - 1
	qos := i.TxQoS
	if qos > 0 {
		rl += 2
		publish |= qos << 1

		if i.Sent != 0 {
			publish |= 0x08 // set DUP if sent before
		}
		now = time.Now().Unix()
		atomic.StoreInt64(&i.Sent, now)
	} else {
		s.decSendQuota()
	}

	pl := len(i.P.Props)
	var untilExpiry uint32

	if i.P.Expiry != 0 {
		if now == 0 {
			now = time.Now().Unix()
		}
		if now >= i.P.Expiry {
			return s.discardPublish(i)
		}
		if s.protoVersion > 4 {
			untilExpiry = uint32(i.P.Expiry - now)
			pl += 5
		}
	}

	tLen := binary.BigEndian.Uint16(i.P.B[1:])
	topicUTF8 := i.P.B[1 : 3+tLen]

	var tAlias uint16
	var haveTAlias, newTAlias bool

	if s.protoVersion > 4 {
		// Topic Alias
		if s.topicAliasMax > 0 {
			t := topicUTF8[2:]
			tStrRead := bytesToStringUnsafe(t)

			s.taToClient.RLock()
			tAlias, haveTAlias = s.taToClient.aliases[tStrRead]
			s.taToClient.RUnlock()

			// save for non-retained msgs
			if !haveTAlias && !i.Retained && atomic.LoadInt32(&s.taToClient.left) > 0 {
				s.taToClient.Lock()
				tAlias, haveTAlias = s.taToClient.aliases[tStrRead]
				if !haveTAlias {
					newLeft := atomic.AddInt32(&s.taToClient.left, -1)
					if newLeft >= 0 {
						tAlias = s.topicAliasMax - uint16(newLeft)
						s.taToClient.aliases[string(t)] = tAlias
						haveTAlias, newTAlias = true, true
					}
				}
				s.taToClient.Unlock()
			}
			if haveTAlias {
				pl += 3
				if !newTAlias {
					rl -= len(t)
				}
			}
		}

		// Subscription Id
		if i.SId != 0 {
			pl += 1 + model.LengthToNumberOfVariableLengthBytes(int(i.SId))
		}

		// Properties Length & Remaining Length
		rl += pl + model.LengthToNumberOfVariableLengthBytes(pl)

		// Too large, discard. v5[MQTT-3.1.2-24]
		if max := s.maxPacketSize; max != 0 && (uint32(rl) > max ||
			uint32(1+rl+model.LengthToNumberOfVariableLengthBytes(rl)) > max) {
			return s.discardPublish(i)
		}
	}

	s.client.txLock.Lock()

	s.client.tx.WriteByte(publish)
	model.VariableLengthEncodeNoAlloc(rl, s.client.tx.WriteByte)

	// Topic Name
	if !haveTAlias || newTAlias {
		s.client.tx.Write(topicUTF8)
	} else {
		s.client.tx.WriteByte(0)
		s.client.tx.WriteByte(0)
	}

	// PId
	if qos > 0 {
		s.client.tx.WriteByte(byte(i.PId >> 8))
		s.client.tx.WriteByte(byte(i.PId))
	}

	// Properties
	if s.protoVersion > 4 {
		model.VariableLengthEncodeNoAlloc(pl, s.client.tx.WriteByte)
		s.client.tx.Write(i.P.Props)

		if untilExpiry != 0 {
			s.client.tx.WriteByte(model.MessageExpiryInterval)
			s.client.tx.WriteByte(byte(untilExpiry >> 24))
			s.client.tx.WriteByte(byte(untilExpiry >> 16))
			s.client.tx.WriteByte(byte(untilExpiry >> 8))
			s.client.tx.WriteByte(byte(untilExpiry))
		}

		if haveTAlias {
			s.client.tx.WriteByte(model.TopicAlias)
			s.client.tx.WriteByte(byte(tAlias >> 8))
			s.client.tx.WriteByte(byte(tAlias))
		}

		if i.SId != 0 {
			s.client.tx.WriteByte(model.SubscriptionIdentifier)
			model.VariableLengthEncodeNoAlloc(int(i.SId), s.client.tx.WriteByte)
		}
	}

	// Payload
	_, err := s.client.tx.Write(i.P.B[3+tLen:])

	s.client.txLock.Unlock()
	s.client.notifyFlusher()

	if qos == 0 {
		s.incSendQuota()

		err := s.client.server.diskDeleteClientMsg(s.client.dbClientId, i.DbMsgTxId)
		if err != nil {
			log.Error(err)
		}

		pid := i.P.DbSPubId
		if i.P.FreeIfLastUser() {
			err := s.client.server.diskDeleteServerMsg(pid)
			if err != nil {
				log.Error(err)
			}
		}
	}

	return err
}

// Discard before sending
func (s *session) discardPublish(i *queue.Item) error {
	i.P.FreeIfLastUser()
	if i.TxQoS > 0 {
		if i.TxQoS == 1 {
			s.client.q1.Remove(i)
		} else {
			s.client.q2.Remove(i)
		}

		queue.ReturnItemQos12(i)
		s.client.pIDs <- i.PId
	}
	return nil
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
	if s.protoVersion == 3 && i.Sent != 0 {
		header |= 0x08 // set mqtt3 DUP
	}

	s.client.txLock.Lock()

	s.client.tx.WriteByte(header)
	s.client.tx.WriteByte(2)
	s.client.tx.WriteByte(byte(i.PId >> 8))
	err := s.client.tx.WriteByte(byte(i.PId))

	s.client.txLock.Unlock()
	s.client.notifyFlusher()

	i.Sent = time.Now().Unix()
	return err
}

func (s *session) sendPubcomp() error {
	// TODO: support Reason Code, Properties
	p := s.packet.payload[:0]
	p = append(p, model.PUBCOMP, 2, s.packet.vhBuf[0], s.packet.vhBuf[1])
	return s.writePacket(p)
}

func (s *session) sendSuback(subRCs []uint8) error {
	rl := len(subRCs) + 2
	if s.protoVersion > 4 {
		rl++
	}

	s.client.txLock.Lock()

	s.client.tx.WriteByte(model.SUBACK)
	model.VariableLengthEncodeNoAlloc(rl, s.client.tx.WriteByte)

	// Variable Header
	s.client.tx.WriteByte(s.packet.vhBuf[0]) // [MQTT-3.8.4-2]
	s.client.tx.WriteByte(s.packet.vhBuf[1])

	if s.protoVersion > 4 {
		s.client.tx.WriteByte(0) // TODO: support properties
	}

	// Payload
	_, err := s.client.tx.Write(subRCs) // [MQTT-3.9.3-1]

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
	model.VariableLengthEncodeNoAlloc(rl, s.client.tx.WriteByte)

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
	ns := session{
		ctx:            ctx,
		cancel:         cancel,
		conn:           conn,
		receiveMax:     65535,
		reqProblemInfo: true,
	}
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
			if err != errGotNormalDiscon {
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
			ns.handleParseError(err)
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
			ns.handleParseError(err)
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

func (s *session) handleParseError(err error) {
	if err == errGotNormalDiscon || err == errGotDisconWithWill {
		return
	}

	log.WithFields(log.Fields{
		"ClientId": s.clientId,
		"err":      err,
	}).Debug("client failure")
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
