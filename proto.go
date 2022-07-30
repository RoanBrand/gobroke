package gobroke

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/RoanBrand/gobroke/internal/model"
	"github.com/RoanBrand/gobroke/internal/queue"
	log "github.com/sirupsen/logrus"
)

// MQTT packet parser states
const (
	// Fixed header
	controlAndFlags = iota
	length

	variableHeaderLen
	variableHeader
	propertiesLenCONNECT
	propertiesCONNECT

	payload
)

var (
	protoVersionToName = map[uint8][]byte{
		3: {'M', 'Q', 'I', 's', 'd', 'p'},
		4: {'M', 'Q', 'T', 'T'},
		5: {'M', 'Q', 'T', 'T'},
	}

	// QoS 1&2 unacknowledged message resend timeout in ms
	// Set to 0 to disable. Will always resend once on new conn.
	pubTimeout uint64 = 50000

	pingRespPacket = []byte{model.PINGRESP, 0}

	// server got DISCONNECT packet. Do not send will on connection close.
	errCleanExit = errors.New("cleanExit")
)

func (s *Server) parseStream(ses *session, rx []byte) error {
	p, l := &ses.packet, uint32(len(rx))
	var i uint32

	for i < l {
		switch ses.rxState {
		case controlAndFlags:
			p.controlType, p.flags = rx[i]&0xF0, rx[i]&0x0F

			switch p.controlType { // [MQTT-2.2.2-1, 2-2]
			case model.PUBLISH:
				rawQoS := p.flags & 0x06
				if rawQoS == 0 {
					if p.flags&0x08 > 0 { // [MQTT-3.3.1-2]
						return errors.New("malformed PUBLISH: DUP set for QoS0")
					}
				} else if rawQoS == 6 { // [MQTT-3.3.1-4]
					return errors.New("malformed PUBLISH: no QoS3 allowed")
				}
			case model.PUBACK, model.PUBREC, model.PUBCOMP, model.PINGREQ:
				if p.flags != 0 {
					return errors.New("malformed packet: fixed header flags must be 0 (reserved)")
				}
			case model.PUBREL:
				f := p.flags
				if ses.protoVersion == 3 {
					f &= 0x07
				}
				if f != 0x02 {
					return errors.New("malformed PUBREL")
				}
			case model.SUBSCRIBE:
				f := p.flags
				if ses.protoVersion == 3 {
					f &= 0x07
				}
				if f != 0x02 { // [MQTT-3.8.1-1]
					return errors.New("malformed SUBSCRIBE")
				}
			case model.UNSUBSCRIBE:
				f := p.flags
				if ses.protoVersion == 3 {
					f &= 0x07
				}
				if f != 0x02 { // [MQTT-3.10.1-1]
					return errors.New("malformed UNSUBSCRIBE")
				}
			case model.DISCONNECT:
				if p.flags != 0 {
					return errors.New("malformed DISCONNECT: fixed header flags must be 0 (reserved)")
				}

				log.WithFields(log.Fields{
					"ClientId": ses.clientId,
				}).Debug("DISCONNECT received")

				return errCleanExit
			case model.CONNECT:
				if ses.connectSent { // [MQTT-3.1.0-2]
					return errors.New("second CONNECT packet received")
				}
				if p.flags != 0 {
					return errors.New("malformed packet: fixed header flags must be 0 (reserved)")
				}
			default:
				return errors.New("invalid MQTT Control Packet type")
			}

			ses.rxState = length
			p.remainingLength, p.lenMul = 0, 1
			i++
		case length:
			p.remainingLength += uint32(rx[i]&127) * p.lenMul
			if p.lenMul *= 128; p.lenMul > 128*128*128 {
				return errors.New("malformed packet: bad remaining length")
			}

			if rx[i]&128 == 0 {
				switch p.controlType {
				case model.PUBLISH, model.CONNECT:
					p.vhLen = 0 // determined later
				case model.PUBACK, model.PUBREC, model.PUBREL, model.PUBCOMP:
					p.vhLen = 2
				case model.SUBSCRIBE:
					if p.remainingLength < 5 { // [MQTT-3.8.3-3]
						return errors.New("malformed SUBSCRIBE: no Topic Filter present")
					}
					p.vhLen = 2
				case model.UNSUBSCRIBE:
					if p.remainingLength < 5 { // [MQTT-3.10.3-2]
						return errors.New("malformed UNSUBSCRIBE: no Topic Filter present")
					}
					p.vhLen = 2
				case model.PINGREQ:
					if err := ses.writePacket(pingRespPacket); err != nil {
						return err
					}
				}

				if p.remainingLength == 0 {
					ses.updateTimeout()
					ses.rxState = controlAndFlags
				} else {
					p.vh = p.vh[:0]
					if p.controlType == model.PUBLISH || p.controlType == model.CONNECT {
						ses.rxState = variableHeaderLen
					} else {
						ses.rxState = variableHeader
					}
				}
			}

			i++
		case variableHeaderLen:
			p.vh = append(p.vh, rx[i])
			p.vhLen++
			p.remainingLength--

			if p.vhLen == 2 {
				p.vhLen = uint32(binary.BigEndian.Uint16(p.vh)) // vhLen is now remaining
				if p.controlType == model.PUBLISH {
					if p.flags&0x06 > 0 { // Qos > 0
						p.vhLen += 2
					}
				} else { // CONNECT
					p.vhLen += 4 // ProtoVersion + ConnectFlags + KeepAlive
				}
				ses.rxState = variableHeader
			}

			i++
		case variableHeader:
			toRead := p.vhLen
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vh = append(p.vh, rx[i:i+toRead]...)
			p.remainingLength -= toRead
			p.vhLen -= toRead

			if p.vhLen == 0 {
				c := ses.client
				switch p.controlType {
				case model.PUBLISH:
					tLen := binary.BigEndian.Uint16(p.vh)
					if tLen > 0 {
						if err := checkUTF8(p.vh[2:2+tLen], true); err != nil { // [MQTT-3.3.2-1]
							return errors.New("malformed PUBLISH: bad Topic Name: " + err.Error())
						}
					}
					if p.flags&0x06 > 0 { // Qos > 0
						p.pID = binary.BigEndian.Uint16(p.vh[len(p.vh)-2:])
					}
				case model.PUBACK:
					c.qos1Done(binary.BigEndian.Uint16(p.vh))
				case model.PUBREC:
					c.acks[0], c.acks[1], c.acks[2], c.acks[3] = model.PUBREL|0x02, 2, p.vh[0], p.vh[1]
					pID := binary.BigEndian.Uint16(p.vh)
					if !c.qos2Part1Done(pID) && ses.protoVersion == 3 && c.q2Stage2.Present(pID) {
						c.acks[0] |= 0x08 // set Dup
					}
					if err := ses.writePacket(c.acks); err != nil {
						return err
					}
				case model.PUBREL: // [MQTT-4.3.3-2]
					delete(c.q2RxLookup, binary.BigEndian.Uint16(p.vh))
					c.acks[0], c.acks[1], c.acks[2], c.acks[3] = model.PUBCOMP, 2, p.vh[0], p.vh[1]
					if err := ses.writePacket(c.acks); err != nil {
						return err
					}
				case model.PUBCOMP:
					c.qos2Part2Done(binary.BigEndian.Uint16(p.vh))
				case model.CONNECT:
					pnLen := binary.BigEndian.Uint16(p.vh)
					ses.protoVersion = p.vh[2+pnLen]

					if !bytes.Equal(protoVersionToName[ses.protoVersion], p.vh[2:2+pnLen]) { // [MQTT-3.1.2-1]
						ses.sendConnack(1, false) // [MQTT-3.1.2-2]
						return errors.New("unsupported client protocol. Must be MQTT v3.1 (3) or v3.1.1 (4)")
					}

					switch ses.protoVersion {
					case 3:
						if p.remainingLength < 3 {
							return errors.New("invalid CONNECT: ClientId must be between 1-23 characters long for MQTT v3.1 (3)")
						}
					case 4:
						if p.remainingLength < 2 { // [MQTT-3.1.3-3]
							return errors.New("malformed CONNECT: absent ClientId in payload")
						}
					case 5:
						if p.remainingLength < 3 {
							return errors.New("malformed CONNECT: too short (missing Properties Length or ClientId")
						}
					}

					ses.connectFlags = p.vh[3+pnLen]
					if ses.connectFlags&0x01 > 0 { // [MQTT-3.1.2-3]
						return errors.New("malformed CONNECT: reserved header flag must be 0")
					}

					// [MQTT-3.1.2-24]
					ses.keepAlive = time.Duration(binary.BigEndian.Uint16(p.vh[4+pnLen:])) * time.Second * 3 / 2
				}

				p.payload = p.payload[:0]
				if p.remainingLength == 0 {
					ses.updateTimeout()
					if p.controlType == model.PUBLISH {
						if err := s.handlePublish(ses); err != nil {
							return err
						}
					}
					ses.rxState = controlAndFlags
				} else {
					if p.controlType == model.CONNECT && ses.protoVersion == 5 {
						p.vhPropertyLen, p.lenMul = 0, 1
						ses.rxState = propertiesLenCONNECT
					} else {
						ses.rxState = payload
					}
				}
			}

			i += toRead
		case payload:
			toRead := p.remainingLength
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.payload = append(p.payload, rx[i:i+toRead]...)
			p.remainingLength -= toRead

			if p.remainingLength == 0 {
				var err error
				switch p.controlType {
				case model.PUBLISH:
					err = s.handlePublish(ses)
				case model.SUBSCRIBE:
					err = s.handleSubscribe(ses)
				case model.UNSUBSCRIBE:
					err = s.handleUnsubscribe(ses)
				case model.CONNECT:
					err = s.handleConnect(ses)
				}
				if err != nil {
					return err
				}

				ses.updateTimeout()
				ses.rxState = controlAndFlags
			}

			i += toRead
		case propertiesLenCONNECT:
			p.vhPropertyLen += uint32(rx[i]&127) * p.lenMul
			if p.lenMul *= 128; p.lenMul > 128*128*128 {
				return errors.New("malformed CONNECT: bad Properties Length")
			}

			if rx[i]&128 == 0 {
				if p.vhPropertyLen == 0 {
					ses.rxState = payload
				} else {
					p.vh = p.vh[:0]
					ses.rxState = propertiesCONNECT
				}
			}

			p.remainingLength--
			i++
		case propertiesCONNECT:
			toRead := p.vhPropertyLen
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vh = append(p.vh, rx[i:i+toRead]...)
			p.remainingLength -= toRead
			p.vhPropertyLen -= toRead

			if p.vhPropertyLen == 0 {
				if err := s.handleConnectProperties(ses); err != nil {
					return err
				}

				ses.rxState = payload
			}

			i += toRead
		}

	}

	return nil
}

var unNamedClients uint32

func (s *Server) handleConnect(ses *session) error {
	p := ses.packet.payload
	pLen := uint32(len(p))

	// ClientId
	clientIdLen := uint32(binary.BigEndian.Uint16(p))
	offs := 2 + clientIdLen
	if pLen < offs {
		return errors.New("malformed CONNECT: payload too short for ClientId")
	}

	if clientIdLen > 0 {
		if ses.protoVersion == 3 && clientIdLen > 23 {
			ses.sendConnack(2, false)
			return errors.New("invalid CONNECT: ClientId must be between 1-23 characters long for MQTT v3.1 (3)")
		}

		if err := checkUTF8(p[2:offs], false); err != nil { // [MQTT-3.1.3-4]
			return errors.New("malformed CONNECT: bad ClientId: " + err.Error())
		}

		ses.clientId = string(p[2:offs])
	} else {
		if ses.persistent() { // [MQTT-3.1.3-7]
			ses.sendConnack(2, false) // [MQTT-3.1.3-8]
			return errors.New("malformed CONNECT: must have ClientId present when persistent session")
		}

		newUnNamed := atomic.AddUint32(&unNamedClients, 1)
		ses.clientId = fmt.Sprintf("noname-%d-%d", newUnNamed, time.Now().UnixNano()/100000)
	}

	// Will Properties, Will Topic & Will Message/Payload
	if ses.connectFlags&0x04 > 0 {
		// Properties
		if ses.protoVersion == 5 {
			len, lenMul := 0, 1
			for done := false; !done; offs++ {
				len += int(p[offs]&127) * lenMul
				if lenMul *= 128; lenMul > 128*128*128 {
					return errors.New("malformed CONNECT: bad Will Properties Length")
				}

				done = p[offs]&128 == 0
			}

			offs += uint32(len) // TODO: store and use Will Properties
		}

		// Topic
		if pLen < 2+offs {
			return errors.New("malformed CONNECT: no Will Topic in payload")
		}

		wTopicUTFStart := offs
		wTopicLen := uint32(binary.BigEndian.Uint16(p[offs:]))
		offs += 2
		if pLen < offs+wTopicLen {
			return errors.New("malformed CONNECT: payload too short for Will Topic")
		}

		wTopicUTFEnd := offs + wTopicLen
		offs += wTopicLen
		if pLen < 2+offs {
			return errors.New("malformed CONNECT: no Will Message in payload")
		}

		wTopicUTF8 := p[wTopicUTFStart:wTopicUTFEnd]
		if err := checkUTF8(wTopicUTF8[2:], true); err != nil { // [MQTT-3.1.3-10]
			return errors.New("malformed CONNECT: bad Will Topic: " + err.Error())
		}

		wMsgLen := uint32(binary.BigEndian.Uint16(p[offs:]))
		offs += 2
		if pLen < offs+wMsgLen {
			return errors.New("malformed CONNECT: payload too short for Will Message")
		}

		wQoS := (ses.connectFlags & 0x18) >> 3
		if wQoS > 2 { // [MQTT-3.1.2-14]
			return errors.New("malformed CONNECT: invalid Will QoS level")
		}

		willPubFlags := wQoS << 1
		if ses.connectFlags&0x20 > 0 {
			willPubFlags |= 0x01 // retain
		}

		ses.will = model.NewPub(willPubFlags, wTopicUTF8, p[offs:offs+wMsgLen])
		offs += wMsgLen

	} else if ses.connectFlags&0x38 > 0 { // [MQTT-3.1.2-11, 2-13, 2-15]
		return errors.New("malformed CONNECT: bad Will Flags")
	}

	// Username & Password
	if ses.connectFlags&0x80 > 0 {
		if pLen < 2+offs {
			return errors.New("malformed CONNECT: no User Name in payload")
		}

		userLen := uint32(binary.BigEndian.Uint16(p[offs:]))
		offs += 2
		if pLen < offs+userLen {
			return errors.New("malformed CONNECT: payload too short for User Name")
		}

		userName := p[offs : offs+userLen]
		if err := checkUTF8(userName, false); err != nil { // [MQTT-3.1.3-11]
			return errors.New("malformed CONNECT: bad User Name: " + err.Error())
		}

		ses.userName = string(userName)
		offs += userLen

		if ses.connectFlags&0x40 > 0 {
			if pLen < 2+offs {
				return errors.New("malformed CONNECT: no Password in payload")
			}

			passLen := uint32(binary.BigEndian.Uint16(p[offs:]))
			offs += 2
			if pLen < offs+passLen {
				return errors.New("malformed CONNECT: payload too short for Password")
			}

			ses.password = make([]byte, passLen)
			copy(ses.password, p[offs:offs+passLen])
			offs += passLen
		}

	} else if ses.connectFlags&0x40 > 0 {
		return errors.New("malformed CONNECT: Password present without User Name")
	}

	if offs != pLen {
		return errors.New("malformed CONNECT: unexpected extra payload fields (Will Topic, Will Message, User Name or Password)")
	}

	if err := ses.conn.SetReadDeadline(time.Time{}); err != nil { // CONNECT packet timeout cancel
		return err
	}

	ses.sendConnack(0, s.addSession(ses)) // [MQTT-3.2.2-1, 2-2, 2-3]
	ses.connectSent = true
	ses.run(pubTimeout)
	return nil
}

func (s *Server) handleConnectProperties(ses *session) error {
	return nil // TODO: store and use Connect Properties
}

func (s *Server) handlePublish(ses *session) error {
	p := &ses.packet
	topicLen := binary.BigEndian.Uint16(p.vh)
	pub := model.NewPub(p.flags, p.vh[:topicLen+2], p.payload)

	if log.IsLevelEnabled(log.DebugLevel) {
		lf := log.Fields{
			"ClientId":   ses.clientId,
			"Topic Name": string(p.vh[2 : topicLen+2]),
			"QoS":        pub.RxQoS(),
		}
		if pub.Duplicate() {
			lf["duplicate"] = true
		}
		if pub.ToRetain() {
			lf["retain"] = true
		}
		log.WithFields(lf).Debug("PUBLISH received")
	}

	c := ses.client
	switch pub.RxQoS() {
	case 0:
		s.pubs.Add(queue.GetItem(pub))
	case 1:
		s.pubs.Add(queue.GetItem(pub))
		c.acks[0], c.acks[1], c.acks[2], c.acks[3] = model.PUBACK, 2, byte(p.pID>>8), byte(p.pID)
		return ses.writePacket(c.acks)
	case 2: // [MQTT-4.3.3-2]
		if _, ok := c.q2RxLookup[p.pID]; !ok {
			c.q2RxLookup[p.pID] = struct{}{}
			s.pubs.Add(queue.GetItem(pub))
		}
		c.acks[0], c.acks[1], c.acks[2], c.acks[3] = model.PUBREC, 2, byte(p.pID>>8), byte(p.pID)
		return ses.writePacket(c.acks)
	}

	return nil
}

func (s *Server) handleSubscribe(ses *session) error {
	p := ses.packet.payload
	topics, qoss := make([][][]byte, 0, 2), make([]uint8, 0, 2)
	i := uint32(0)

	for i < uint32(len(p)) {
		topicL := uint32(binary.BigEndian.Uint16(p[i:]))
		i += 2
		topicEnd := i + topicL
		if p[topicEnd]&0xFC != 0 { // [MQTT-3-8.3-4]
			return errors.New("malformed SUBSCRIBE: Reserved bits in payload must be 0")
		}

		topic := p[i:topicEnd]
		if err := checkUTF8(topic, false); err != nil { // [MQTT-3.8.3-1]
			return errors.New("malformed SUBSCRIBE: bad Topic Filter: " + err.Error())
		}

		if log.IsLevelEnabled(log.DebugLevel) {
			log.WithFields(log.Fields{
				"ClientId":     ses.clientId,
				"Topic Filter": string(topic),
			}).Debug("SUBSCRIBE received")
		}

		topics, qoss = append(topics, bytes.Split(topic, []byte{'/'})), append(qoss, p[topicEnd])
		i += 1 + topicL
	}

	s.addSubscriptions(ses.client, topics, qoss)

	// [MQTT-3.8.4-1, 4-4, 4-5, 4-6]
	tl := len(topics)
	subackP := make([]byte, 1, tl+7)
	subackP[0] = model.SUBACK
	subackP = model.VariableLengthEncode(subackP, tl+2)
	subackP = append(subackP, ses.packet.vh[0], ses.packet.vh[1]) // [MQTT-3.8.4-2]
	subackP = append(subackP, qoss...)                            // [MQTT-3.9.3-1]

	return ses.writePacket(subackP)
}

func (s *Server) handleUnsubscribe(ses *session) error {
	p := ses.packet.payload
	topics := make([][][]byte, 0, 2)
	i := uint32(0)

	for i < uint32(len(p)) {
		topicL := uint32(binary.BigEndian.Uint16(p[i:]))
		i += 2
		topicEnd := i + topicL
		topic := p[i:topicEnd]
		if err := checkUTF8(topic, false); err != nil { // [MQTT-3.10.3-1]
			return errors.New("malformed UNSUBSCRIBE: bad Topic Filter: " + err.Error())
		}

		if log.IsLevelEnabled(log.DebugLevel) {
			log.WithFields(log.Fields{
				"ClientId":     ses.clientId,
				"Topic Filter": string(topic),
			}).Debug("UNSUBSCRIBE received")
		}

		topics = append(topics, bytes.Split(topic, []byte{'/'}))
		i += topicEnd
	}

	c := ses.client
	s.removeSubscriptions(c, topics)

	// [MQTT-3.10.4-4, 4-5, 4-6]
	c.acks[0], c.acks[1], c.acks[2], c.acks[3] = model.UNSUBACK, 2, ses.packet.vh[0], ses.packet.vh[1]
	return ses.writePacket(c.acks)
}

var errInvalidUTF = errors.New("invalid UTF8")
var errContainsWildCards = errors.New("contains wildcard characters")

// [MQTT-1.5.3-1] [MQTT-1.5.3-3]
func checkUTF8(str []byte, checkWildCards bool) error {
	for i := 0; i < len(str); {
		if str[i] == 0 { // [MQTT-1.5.3-2]
			return errInvalidUTF
		}

		if checkWildCards && (str[i] == '+' || str[i] == '#') { // [MQTT-3.3.2-2]
			return errContainsWildCards
		} else if str[i]&0x80 == 0 {
			i++
		} else {
			r, size := utf8.DecodeRune(str[i:])
			if r == utf8.RuneError {
				if size != 1 {
					return nil
				} else {
					return errInvalidUTF
				}
			}
			i += size
		}
	}
	return nil
}
