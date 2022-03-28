package broker

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/RoanBrand/gobroke/internal/queue"
	log "github.com/sirupsen/logrus"
)

var errCleanExit = errors.New("cleanExit")

func protocolViolation(msg string) error {
	return errors.New("client protocol violation: " + msg)
}

func (s *Server) parseStream(ses *session, rx []byte) error {
	p, l := &ses.packet, uint32(len(rx))
	var i uint32

	for i < l {
		switch ses.rxState {
		case controlAndFlags:
			p.controlType, p.flags = rx[i]&0xF0, rx[i]&0x0F

			if p.controlType < CONNECT || p.controlType > DISCONNECT {
				return protocolViolation("invalid control packet")
			}

			// handle first and only connect
			if ses.connectSent {
				if p.controlType == CONNECT { // [MQTT-3.1.0-2]
					return protocolViolation("second CONNECT packet")
				}
			} else {
				if p.controlType != CONNECT { // [MQTT-3.1.0-1]
					return protocolViolation("first packet not CONNECT")
				}
			}

			switch p.controlType { // [MQTT-2.2.2-1, 2-2]
			case CONNECT, PUBACK, PUBREC, PUBCOMP, PINGREQ:
				if p.flags != 0 {
					return protocolViolation("malformed packet - Fixed header flags must be 0 (reserved)")
				}
			case PUBLISH:
				if (p.flags&0x08 > 0) && (p.flags&0x06 == 0) { // [MQTT-3.3.1-2]
					return protocolViolation("malformed PUBLISH - DUP set for QoS0 Pub")
				}
				if p.flags&0x06 == 6 { // [MQTT-3.3.1-4]
					return protocolViolation("malformed PUBLISH - No QoS3")
				}
			case PUBREL:
				if p.flags != 0x02 {
					return protocolViolation("malformed PUBREL")
				}
			case SUBSCRIBE:
				if p.flags != 0x02 { // [MQTT-3.8.1-1]
					return protocolViolation("malformed SUBSCRIBE")
				}
			case UNSUBSCRIBE:
				if p.flags != 0x02 { // [MQTT-3.10.1-1]
					return protocolViolation("malformed UNSUBSCRIBE")
				}
			case DISCONNECT:
				if p.flags != 0 {
					return protocolViolation("malformed DISCONNECT - Fixed header flags must be 0 (reserved)")
				}

				log.WithFields(log.Fields{
					"client": ses.clientId,
				}).Debug("Got DISCONNECT packet")

				return errCleanExit
			}

			p.lenMul = 1
			p.remainingLength = 0
			ses.rxState = length
			i++
		case length:
			p.remainingLength += uint32(rx[i]&127) * p.lenMul
			p.lenMul *= 128
			if p.lenMul > 128*128*128 {
				return protocolViolation("malformed remaining length")
			}

			if rx[i]&128 == 0 {
				switch p.controlType {
				case CONNECT:
					if p.remainingLength < 12 { // [MQTT-3.1.3-3]
						return protocolViolation("invalid CONNECT - absent clientId in payload")
					}
					p.vhLen = 10
				case PUBLISH:
					p.vhLen = 0 // determined later
				case PUBACK, PUBREC, PUBREL, PUBCOMP:
					p.vhLen = 2
				case SUBSCRIBE:
					if p.remainingLength < 5 { // [MQTT-3.8.3-3]
						return protocolViolation("invalid SUBSCRIBE - no topic filter")
					}
					p.vhLen = 2
				case UNSUBSCRIBE:
					if p.remainingLength < 5 { // [MQTT-3.10.3-2]
						return protocolViolation("invalid UNSUBSCRIBE - no topic filter")
					}
					p.vhLen = 2
				case PINGREQ:
					if err := ses.writePacket(pingRespPacket); err != nil {
						return err
					}
				}

				if p.remainingLength == 0 {
					ses.updateTimeout()
					ses.rxState = controlAndFlags
				} else {
					p.vh = p.vh[:0]
					if p.controlType == PUBLISH {
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
				if p.flags&0x06 > 0 {                           // Qos > 0
					p.vhLen += 2
				}
				ses.rxState = variableHeader
			}

			i++
		case variableHeader:
			avail := l - i
			toRead := p.vhLen
			if avail < toRead {
				toRead = avail
			}

			p.vh = append(p.vh, rx[i:i+toRead]...)
			p.remainingLength -= toRead
			p.vhLen -= toRead

			if p.vhLen == 0 {
				c := ses.client
				switch p.controlType {
				case CONNECT:
					if !bytes.Equal(connectPacket, p.vh[:7]) { // [MQTT-3.1.2-1]
						ses.sendConnack(1, false) // [MQTT-3.1.2-2]
						return protocolViolation("unsupported client protocol. Must be MQTT v3.1.1")
					}

					ses.connectFlags = p.vh[7]
					if ses.connectFlags&0x01 > 0 { // [MQTT-3.1.2-3]
						return protocolViolation("malformed CONNECT")
					}

					// [MQTT-3.1.2-24]
					ses.keepAlive = time.Duration(binary.BigEndian.Uint16(p.vh[8:])) * time.Second * 3 / 2
				case PUBLISH:
					tLen := uint32(binary.BigEndian.Uint16(p.vh))
					if tLen > 0 {
						if err := checkUTF8(p.vh[2:2+tLen], true); err != nil { // [MQTT-3.3.2-1]
							return protocolViolation("Invalid Publish Topic string: " + err.Error())
						}
					}
					if p.flags&0x06 > 0 { // Qos > 0
						p.pID = binary.BigEndian.Uint16(p.vh[len(p.vh)-2:])
					}
				case PUBACK:
					c.qos1Done(binary.BigEndian.Uint16(p.vh))
				case PUBREC:
					c.acks[0], c.acks[1], c.acks[2], c.acks[3] = PUBREL|0x02, 2, p.vh[0], p.vh[1]
					if err := ses.writePacket(c.acks); err != nil {
						return err
					}
					c.qos2Part1Done(binary.BigEndian.Uint16(p.vh))
				case PUBREL: // [MQTT-4.3.3-2]
					delete(c.q2RxLookup, binary.BigEndian.Uint16(p.vh))
					c.acks[0], c.acks[1], c.acks[2], c.acks[3] = PUBCOMP, 2, p.vh[0], p.vh[1]
					if err := ses.writePacket(c.acks); err != nil {
						return err
					}
				case PUBCOMP:
					c.qos2Part2Done(binary.BigEndian.Uint16(p.vh))
				}

				p.payload = p.payload[:0]
				if p.remainingLength == 0 {
					ses.updateTimeout()
					if p.controlType == PUBLISH {
						if err := s.handlePublish(ses); err != nil {
							return err
						}
					}
					ses.rxState = controlAndFlags
				} else {
					ses.rxState = payload
				}
			}

			i += toRead
		case payload:
			avail := l - i
			toRead := p.remainingLength
			if avail < toRead {
				toRead = avail
			}

			p.payload = append(p.payload, rx[i:i+toRead]...)
			p.remainingLength -= toRead

			if p.remainingLength == 0 {
				var err error
				switch p.controlType {
				case CONNECT:
					err = s.handleConnect(ses)
				case PUBLISH:
					err = s.handlePublish(ses)
				case SUBSCRIBE:
					err = s.handleSubscribe(ses)
				case UNSUBSCRIBE:
					err = s.handleUnsubscribe(ses)
				}
				if err != nil {
					return err
				}

				ses.updateTimeout()
				ses.rxState = controlAndFlags
			}

			i += toRead
		}
	}

	return nil
}

var unNamedClients int

func (s *Server) handleConnect(ses *session) error {
	p := ses.packet.payload
	pLen := uint32(len(p))

	// Client ID
	clientIdLen := uint32(binary.BigEndian.Uint16(p))
	offs := 2 + clientIdLen
	if pLen < offs {
		return protocolViolation("malformed CONNECT payload too short clientID")
	}

	if clientIdLen > 0 {
		if err := checkUTF8(p[2:offs], false); err != nil { // [MQTT-3.1.3-4]
			return protocolViolation("malformed CONNECT clientID: " + err.Error())
		}

		ses.clientId = string(p[2:offs])
	} else {
		if ses.persistent() { // [MQTT-3.1.3-7]
			ses.sendConnack(2, false) // [MQTT-3.1.3-8]
			return protocolViolation("must have clientID when persistent session")
		}

		ses.clientId = fmt.Sprintf("noname-%d-%d", unNamedClients, time.Now().UnixNano()/100000)
		unNamedClients++
	}

	// Will Topic & Msg
	if ses.connectFlags&0x04 > 0 {
		if pLen < 2+offs {
			return protocolViolation("malformed CONNECT payload no will Topic")
		}

		wTopicUTFStart := offs
		wTopicLen := uint32(binary.BigEndian.Uint16(p[offs:]))
		offs += 2
		if pLen < offs+wTopicLen {
			return protocolViolation("malformed CONNECT payload too short will Topic")
		}

		wTopicUTFEnd := offs + wTopicLen
		offs += wTopicLen
		if pLen < 2+offs {
			return protocolViolation("malformed CONNECT payload no will Message")
		}

		wTopicUTF8 := p[wTopicUTFStart:wTopicUTFEnd]
		if err := checkUTF8(wTopicUTF8[2:], true); err != nil { // [MQTT-3.1.3-10]
			return protocolViolation("malformed CONNECT Will Topic string: " + err.Error())
		}

		wMsgLen := uint32(binary.BigEndian.Uint16(p[offs:]))
		offs += 2
		if pLen < offs+wMsgLen {
			return protocolViolation("malformed CONNECT payload too short will Message")
		}

		wQoS := (ses.connectFlags & 0x18) >> 3
		if wQoS > 2 { // [MQTT-3.1.2-14]
			return protocolViolation("malformed CONNECT invalid will QoS level")
		}

		ses.will = makePub(wTopicUTF8, p[offs:offs+wMsgLen], wQoS, ses.connectFlags&0x20 > 0)
		offs += wMsgLen

	} else if ses.connectFlags&0x38 > 0 { // [MQTT-3.1.2-11, 2-13, 2-15]
		return protocolViolation("malformed CONNECT will Flags")
	}

	// Username & Password
	if ses.connectFlags&0x80 > 0 {
		if pLen < 2+offs {
			return protocolViolation("malformed CONNECT payload no username")
		}

		userLen := uint32(binary.BigEndian.Uint16(p[offs:]))
		offs += 2
		if pLen < offs+userLen {
			return protocolViolation("malformed CONNECT payload too short username")
		}

		userName := p[offs : offs+userLen]
		if err := checkUTF8(userName, false); err != nil { // [MQTT-3.1.3-11]
			return protocolViolation("malformed CONNECT User Name: " + err.Error())
		}

		ses.userName = string(userName)
		offs += userLen

		if ses.connectFlags&0x40 > 0 {
			if pLen < 2+offs {
				return protocolViolation("malformed CONNECT payload no password")
			}

			passLen := uint32(binary.BigEndian.Uint16(p[offs:]))
			offs += 2
			if pLen < offs+passLen {
				return protocolViolation("malformed CONNECT payload too short password")
			}

			ses.password = make([]byte, passLen)
			copy(ses.password, p[offs:offs+passLen])
			offs += passLen
		}

	} else if ses.connectFlags&0x40 > 0 {
		return protocolViolation("malformed CONNECT password without username")
	}

	if offs != pLen {
		return protocolViolation("malformed CONNECT: unexpected extra payload fields (Will Topic, Will Message, User Name or Password)")
	}

	if err := ses.conn.SetReadDeadline(time.Time{}); err != nil { // CONNECT packet timeout cancel
		return err
	}

	ses.sendConnack(0, s.addSession(ses)) // [MQTT-3.2.2-1, 2-2, 2-3]
	ses.connectSent = true
	ses.run(s.config.MQTT.RetryInterval)
	return nil
}

func (s *Server) handlePublish(ses *session) error {
	p := &ses.packet
	topicLen := uint32(binary.BigEndian.Uint16(p.vh))
	qos, retain := (p.flags&0x06)>>1, p.flags&0x01 > 0

	if log.IsLevelEnabled(log.DebugLevel) {
		lf := log.Fields{
			"client":  ses.clientId,
			"topic":   string(p.vh[2 : topicLen+2]),
			"QoS":     qos,
			"payload": string(p.payload),
		}
		if p.flags&0x08 > 0 {
			lf["duplicate"] = true
		}
		if retain {
			lf["retain"] = true
		}
		log.WithFields(lf).Debug("Got PUBLISH packet")
	}

	c := ses.client
	switch p.flags & 0x06 {
	case 0x00: // QoS 0
		s.pubs.Add(&queue.Item{P: makePub(p.vh[:topicLen+2], p.payload, qos, retain)})
	case 0x02: // QoS 1
		s.pubs.Add(&queue.Item{P: makePub(p.vh[:topicLen+2], p.payload, qos, retain)})
		c.acks[0], c.acks[1], c.acks[2], c.acks[3] = PUBACK, 2, byte(p.pID>>8), byte(p.pID)
		return ses.writePacket(c.acks)
	case 0x04: // QoS 2 - [MQTT-4.3.3-2]
		if _, ok := c.q2RxLookup[p.pID]; !ok {
			c.q2RxLookup[p.pID] = struct{}{}
			s.pubs.Add(&queue.Item{P: makePub(p.vh[:topicLen+2], p.payload, qos, retain)})
		}
		c.acks[0], c.acks[1], c.acks[2], c.acks[3] = PUBREC, 2, byte(p.pID>>8), byte(p.pID)
		return ses.writePacket(c.acks)
	}

	return nil
}

func (s *Server) handleSubscribe(ses *session) error {
	p := ses.packet.payload
	topics, qoss := make([][]string, 0, 2), make([]uint8, 0, 2)
	i := uint32(0)

	for i < uint32(len(p)) {
		topicL := uint32(binary.BigEndian.Uint16(p[i:]))
		i += 2
		topicEnd := i + topicL
		if p[topicEnd]&0xFC != 0 { // [MQTT-3-8.3-4]
			return protocolViolation("malformed SUBSCRIBE")
		}

		topic := p[i:topicEnd]
		if err := checkUTF8(topic, false); err != nil { // [MQTT-3.8.3-1]
			return protocolViolation("malformed SUBSCRIBE Topic Filter string: " + err.Error())
		}

		topics, qoss = append(topics, strings.Split(string(topic), "/")), append(qoss, p[topicEnd])
		i += 1 + topicL
	}

	log.WithFields(log.Fields{
		"client": ses.clientId,
		"topics": topics,
	}).Debug("Got SUBSCRIBE packet")

	s.addSubscriptions(ses.client, topics, qoss)

	// [MQTT-3.8.4-1, 4-4, 4-5, 4-6]
	tl := len(topics)
	subackP := make([]byte, 1, tl+7)
	subackP[0] = SUBACK
	subackP = variableLengthEncode(subackP, tl+2)
	subackP = append(subackP, ses.packet.vh[0], ses.packet.vh[1]) // [MQTT-3.8.4-2]
	subackP = append(subackP, qoss...)                            // [MQTT-3.9.3-1]

	return ses.writePacket(subackP)
}

func (s *Server) handleUnsubscribe(ses *session) error {
	p := ses.packet.payload
	topics := make([][]string, 0, 2)
	i := uint32(0)

	for i < uint32(len(p)) {
		topicL := uint32(binary.BigEndian.Uint16(p[i:]))
		i += 2
		topicEnd := i + topicL
		topic := p[i:topicEnd]
		if err := checkUTF8(topic, false); err != nil { // [MQTT-3.10.3-1]
			return protocolViolation("malformed UNSUBSCRIBE Topic Filter string: " + err.Error())
		}

		topics = append(topics, strings.Split(string(topic), "/"))
		i += topicEnd
	}

	log.WithFields(log.Fields{
		"client": ses.clientId,
		"topics": topics,
	}).Debug("Got UNSUBSCRIBE packet")

	c := ses.client
	s.removeSubscriptions(c, topics)

	// [MQTT-3.10.4-4, 4-5, 4-6]
	c.acks[0], c.acks[1], c.acks[2], c.acks[3] = UNSUBACK, 2, ses.packet.vh[0], ses.packet.vh[1]
	return ses.writePacket(c.acks)
}

func variableLengthEncode(packet []byte, l int) []byte {
	for {
		eb := l % 128
		l /= 128
		if l > 0 {
			eb |= 128
		}
		packet = append(packet, byte(eb))
		if l <= 0 {
			break
		}
	}
	return packet
}

func variableLengthEncodeNoAlloc(l int, f func(eb byte) error) error {
	for {
		eb := l % 128
		l /= 128
		if l > 0 {
			eb |= 128
		}
		if err := f(byte(eb)); err != nil { // No new memory allocation
			return err
		}
		if l <= 0 {
			return nil
		}
	}
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
