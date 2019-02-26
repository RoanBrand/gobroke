package broker

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

var errCleanExit = errors.New("cleanExit")

func protocolViolation(msg string) error {
	return errors.New("client protocol violation: " + msg)
}

func (s *Server) parseStream(ses *session, rx []byte) error {
	p := &ses.packet
	l := uint32(len(rx))
	var i uint32

	for i < l {
		switch ses.rxState {
		case controlAndFlags:
			p.controlType = rx[i] & 0xF0
			p.flags = rx[i] & 0x0F
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

			switch p.controlType {
			case PUBLISH:
				if (p.flags&0x08 > 0) && (p.flags&0x06 == 0) { // [MQTT-3.3.1-2]
					return protocolViolation("malformed PUBLISH")
				}
				if p.flags&0x06 == 6 { // [MQTT-3.3.1-4]
					return protocolViolation("malformed PUBLISH")
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
					p.vhLen = 10
				case PUBLISH:
					p.vhLen = 0 // determined later
				case PUBACK, PUBREC, PUBREL, PUBCOMP:
					p.vhLen = 2
				case SUBSCRIBE:
					if p.remainingLength < 5 { // [MQTT-3.8.3-3]
						return protocolViolation("invalid SUBSCRIBE")
					}
					p.vhLen = 2
				case UNSUBSCRIBE:
					if p.remainingLength < 5 { // [MQTT-3.10.3-2]
						return protocolViolation("invalid UNSUBSCRIBE")
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
				switch p.controlType {
				case CONNECT:
					if !bytes.Equal(connectPacket, p.vh[:7]) { // [MQTT-3.1.2-1]
						ses.sendConnack(1) // [MQTT-3.1.2-2]
						return protocolViolation("unsupported client protocol. Must be MQTT v3.1.1")
					}

					ses.connectFlags = p.vh[7]
					if ses.connectFlags&0x01 > 0 { // [MQTT-3.1.2-3]
						return protocolViolation("malformed CONNECT")
					}

					// [MQTT-3.1.2-24]
					ses.keepAlive = time.Duration(binary.BigEndian.Uint16(p.vh[8:])) * time.Second * 3 / 2
				case PUBLISH:
					if p.flags&0x06 > 0 { // Qos > 0
						p.pID = binary.BigEndian.Uint16(p.vh[len(p.vh)-2:])
					}
				case PUBACK:
					ses.client.qos1Done(binary.BigEndian.Uint16(p.vh))
				case PUBREC:
					pubRel := []byte{PUBREL | 0x02, 2, p.vh[0], p.vh[1]}
					ses.client.qos2Part1Done(binary.BigEndian.Uint16(p.vh), pubRel)
					if err := ses.writePacket(pubRel); err != nil {
						return err
					}
				case PUBREL: // [MQTT-4.3.3-2]
					delete(ses.client.q2RxLookup, binary.BigEndian.Uint16(p.vh))
					if err := ses.writePacket([]byte{PUBCOMP, 2, p.vh[0], p.vh[1]}); err != nil {
						return err
					}
				case PUBCOMP:
					ses.client.qos2Part2Done(binary.BigEndian.Uint16(p.vh))
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
	pLen := len(p)

	// Client ID
	if pLen < 2 { // [MQTT-3.1.3-3]
		return protocolViolation("malformed CONNECT payload no clientID")
	}
	clientIdLen := int(binary.BigEndian.Uint16(p))
	offs := 2 + clientIdLen
	if pLen < offs {
		return protocolViolation("malformed CONNECT payload too short clientID")
	}

	if clientIdLen > 0 {
		ses.clientId = string(p[2:offs])
	} else {
		if ses.persistent() { // [MQTT-3.1.3-7]
			ses.sendConnack(2) // [MQTT-3.1.3-8]
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
		wTopicLen := int(binary.BigEndian.Uint16(p[offs:]))
		offs += 2
		if pLen < offs+wTopicLen {
			return protocolViolation("malformed CONNECT payload too short will Topic")
		}

		wTopicUTFEnd := offs + wTopicLen
		offs += wTopicLen
		if pLen < 2+offs {
			return protocolViolation("malformed CONNECT payload no will Message")
		}

		wMsgLen := int(binary.BigEndian.Uint16(p[offs:]))
		offs += 2
		if pLen < offs+wMsgLen {
			return protocolViolation("malformed CONNECT payload too short will Message")
		}

		wQoS := (ses.connectFlags & 0x18) >> 3
		if wQoS > 2 { // [MQTT-3.1.2-14]
			return protocolViolation("malformed CONNECT invalid will QoS level")
		}

		ses.will = makePub(p[wTopicUTFStart:wTopicUTFEnd], p[offs:offs+wMsgLen], wQoS, ses.connectFlags&0x20 > 0)
		offs += wMsgLen

	} else if ses.connectFlags&0x38 > 0 { // [MQTT-3.1.2-11, 2-13, 2-15]
		return protocolViolation("malformed CONNECT will Flags")
	}

	// Username & Password
	if ses.connectFlags&0x80 > 0 {
		if pLen < 2+offs {
			return protocolViolation("malformed CONNECT payload no username")
		}
		userLen := int(binary.BigEndian.Uint16(p[offs:]))
		offs += 2
		if pLen < offs+userLen {
			return protocolViolation("malformed CONNECT payload too short username")
		}

		ses.userName = string(p[offs : offs+userLen])
		offs += userLen

		if ses.connectFlags&0x40 > 0 {
			if pLen < 2+offs {
				return protocolViolation("malformed CONNECT payload no password")
			}
			passLen := int(binary.BigEndian.Uint16(p[offs:]))
			offs += 2
			if pLen < offs+passLen {
				return protocolViolation("malformed CONNECT payload too short password")
			}

			ses.password = make([]byte, passLen)
			copy(ses.password, p[offs:offs+passLen])
			// offs += wMsgLen
		}

	} else if ses.connectFlags&0x40 > 0 {
		return protocolViolation("malformed CONNECT password without username")
	}

	ses.conn.SetReadDeadline(time.Time{}) // CONNECT packet timeout cancel
	ses.connectSent = true
	s.register <- ses
	return nil
}

func (s *Server) handlePublish(ses *session) error {
	p := &ses.packet
	topicLen := int(binary.BigEndian.Uint16(p.vh))
	topic := string(p.vh[2 : topicLen+2])
	qos := (p.flags & 0x06) >> 1
	retain := p.flags&0x01 > 0

	lf := log.Fields{
		"client":  ses.clientId,
		"topic":   topic,
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

	switch p.flags & 0x06 {
	case 0x00: // QoS 0
		s.pubs <- makePub(p.vh[:topicLen+2], p.payload, qos, retain)
	case 0x02: // QoS 1
		s.pubs <- makePub(p.vh[:topicLen+2], p.payload, qos, retain)
		return ses.writePacket([]byte{PUBACK, 2, byte(p.pID >> 8), byte(p.pID)})
	case 0x04: // QoS 2 - [MQTT-4.3.3-2]
		if _, present := ses.client.q2RxLookup[p.pID]; !present {
			ses.client.q2RxLookup[p.pID] = struct{}{}
			s.pubs <- makePub(p.vh[:topicLen+2], p.payload, qos, retain)
		}
		return ses.writePacket([]byte{PUBREC, 2, byte(p.pID >> 8), byte(p.pID)})
	}

	return nil
}

func (s *Server) handleSubscribe(ses *session) error {
	p := ses.packet.payload
	topics := make([]string, 0, 2)
	qoss := make([]uint8, 0, 2)
	i := 0

	for i < len(p) {
		topicL := int(binary.BigEndian.Uint16(p[i:]))
		topicEnd := i + 2 + topicL
		topics = append(topics, string(p[i+2:topicEnd]))
		if p[topicEnd]&0xFC != 0 { // [MQTT-3-8.3-4]
			return protocolViolation("malformed SUBSCRIBE")
		}

		qoss = append(qoss, p[topicEnd])
		i += 3 + topicL
	}

	log.WithFields(log.Fields{
		"client": ses.clientId,
		"topics": topics,
	}).Debug("Got SUBSCRIBE packet")

	s.subscribe <- subList{ses.client, topics, qoss}

	// [MQTT-3.8.4-1, 4-4, 4-5, 4-6]
	tl := len(topics)
	subackP := make([]byte, 1, tl+7)
	subackP[0] = SUBACK
	subackP = append(subackP, variableLengthEncode(tl+2)...)
	subackP = append(subackP, ses.packet.vh[0], ses.packet.vh[1]) // [MQTT-3.8.4-2]
	subackP = append(subackP, qoss...)                            // [MQTT-3.9.3-1]

	return ses.writePacket(subackP)
}

func (s *Server) handleUnsubscribe(ses *session) error {
	p := ses.packet.payload
	topics := make([]string, 0, 2)
	i := 0

	for i < len(p) {
		topicL := int(binary.BigEndian.Uint16(p[i:]))
		topicEnd := i + 2 + topicL
		topics = append(topics, string(p[i+2:topicEnd]))
		i += 2 + topicEnd
	}

	log.WithFields(log.Fields{
		"client": ses.clientId,
		"topics": topics,
	}).Debug("Got UNSUBSCRIBE packet")

	s.unsubscribe <- subList{ses.client, topics, nil}

	// [MQTT-3.10.4-4, 4-5, 4-6]
	return ses.writePacket([]byte{UNSUBACK, 2, ses.packet.vh[0], ses.packet.vh[1]})
}

func variableLengthEncode(l int) []byte {
	res := make([]byte, 0, 2)
	for {
		eb := l % 128
		l /= 128
		if l > 0 {
			eb |= 128
		}
		res = append(res, byte(eb))
		if l <= 0 {
			break
		}
	}
	return res
}
