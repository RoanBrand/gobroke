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

	// mqtt5
	propertiesLen
	propertiesCONNECT
	propertiesPUBLISH
	propertiesPUBACK
	propertiesPUBREC
	propertiesPUBREL
	propertiesPUBCOMP
	propertiesSUBSCRIBE
	propertiesUNSUBSCRIBE

	payload
)

const maxVarLenMul = 128 * 128 * 128

var (
	protoVersionToName = map[uint8][]byte{
		3: {'M', 'Q', 'I', 's', 'd', 'p'},
		4: {'M', 'Q', 'T', 'T'},
		5: {'M', 'Q', 'T', 'T'},
	}

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
				break
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
					return errors.New("malformed SUBSCRIBE: bad fixed header reserved flags")
				}
			case model.UNSUBSCRIBE:
				f := p.flags
				if ses.protoVersion == 3 {
					f &= 0x07
				}
				if f != 0x02 { // [MQTT-3.10.1-1]
					return errors.New("malformed UNSUBSCRIBE: bad fixed header reserved flags")
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
			p.lenMul = 1
			i++
		case length:
			p.remainingLength += uint32(rx[i]&127) * p.lenMul
			if p.lenMul *= 128; p.lenMul > maxVarLenMul {
				return errors.New("malformed packet: bad remaining length")
			}

			if rx[i]&128 == 0 {
				switch p.controlType {
				case model.PUBLISH, model.CONNECT:
					p.vhToRead = 2
					p.vhBuf = p.vhBuf[:0]
					ses.rxState = variableHeaderLen
				case model.PUBACK, model.PUBREC, model.PUBREL, model.PUBCOMP:
					p.vhToRead = 2
					if ses.protoVersion == 5 && p.remainingLength > 2 {
						p.vhToRead++ // Reason Code
					}

					p.vhBuf = p.vhBuf[:0]
					ses.rxState = variableHeader
				case model.SUBSCRIBE:
					// [MQTT-3.8.3-3]
					if p.remainingLength < 5 || (ses.protoVersion == 5 && p.remainingLength < 6) {
						return errors.New("malformed SUBSCRIBE: no Topic Filter present")
					}

					p.vhToRead = 2
					p.vhBuf = p.vhBuf[:0]
					ses.rxState = variableHeader
				case model.UNSUBSCRIBE:
					if p.remainingLength < 4 || (ses.protoVersion == 5 && p.remainingLength < 5) { // [MQTT-3.10.3-2]
						return errors.New("malformed UNSUBSCRIBE: no Topic Filter present")
					}

					p.vhToRead = 2
					p.vhBuf = p.vhBuf[:0]
					ses.rxState = variableHeader
				case model.PINGREQ:
					if err := ses.writePacket(pingRespPacket); err != nil {
						return err
					}

					ses.updateTimeout()
					ses.rxState = controlAndFlags
				}
			}

			i++
		case variableHeaderLen:
			toRead := p.vhToRead
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vhBuf = append(p.vhBuf, rx[i:i+toRead]...)
			p.vhToRead -= toRead
			p.remainingLength -= toRead

			if p.vhToRead == 0 {
				p.vhToRead = uint32(binary.BigEndian.Uint16(p.vhBuf))
				if p.controlType == model.PUBLISH {
					rawQoS := p.flags & 0x06
					if rawQoS > 0 { // Qos > 0
						if rawQoS == 6 { // [MQTT-3.3.1-4]
							return errors.New("malformed PUBLISH: no QoS3 allowed")
						}

						p.vhToRead += 2
					} else if p.flags&0x08 > 0 { // [MQTT-3.3.1-2]
						return errors.New("malformed PUBLISH: DUP set for QoS0")
					}

				} else { // CONNECT
					p.vhToRead += 4 // ProtoVersion + ConnectFlags + KeepAlive
				}
				ses.rxState = variableHeader
			}

			i += toRead
		case variableHeader:
			toRead := p.vhToRead
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vhBuf = append(p.vhBuf, rx[i:i+toRead]...)
			p.remainingLength -= toRead
			p.vhToRead -= toRead

			if p.vhToRead == 0 {
				c := ses.client
				switch p.controlType {
				case model.PUBLISH:
					tLen := binary.BigEndian.Uint16(p.vhBuf)
					if tLen > 0 {
						if err := checkUTF8(p.vhBuf[2:2+tLen], true); err != nil { // [MQTT-3.3.2-1]
							return errors.New("malformed PUBLISH: bad Topic Name: " + err.Error())
						}
					}
					if p.flags&0x06 > 0 { // Qos > 0
						p.pID = binary.BigEndian.Uint16(p.vhBuf[2+tLen:])
					}
				case model.PUBACK:
					if p.remainingLength == 0 {
						c.qos1Done(binary.BigEndian.Uint16(p.vhBuf))
					}
					if len(p.vhBuf) > 2 && ses.protoVersion == 5 && p.vhBuf[2] != 0 {
						log.WithFields(log.Fields{
							"ClientId":    ses.clientId,
							"Reason Code": p.vhBuf[2],
						}).Debug("PUBACK received")
					}
				case model.PUBREC:
					if p.remainingLength == 0 {
						if err := ses.handlePubrec(); err != nil {
							return err
						}
					}
					if len(p.vhBuf) > 2 && ses.protoVersion == 5 && p.vhBuf[2] != 0 {
						log.WithFields(log.Fields{
							"ClientId":    ses.clientId,
							"Reason Code": p.vhBuf[2],
						}).Debug("PUBREC received")
					}
				case model.PUBREL: // [MQTT-4.3.3-2]
					if p.remainingLength == 0 {
						if err := ses.handlePubrel(); err != nil {
							return err
						}
					}
					if len(p.vhBuf) > 2 && ses.protoVersion == 5 && p.vhBuf[2] != 0 {
						log.WithFields(log.Fields{
							"ClientId":    ses.clientId,
							"Reason Code": p.vhBuf[2],
						}).Debug("PUBREL received")
					}
				case model.PUBCOMP:
					if p.remainingLength == 0 {
						c.qos2Part2Done(binary.BigEndian.Uint16(p.vhBuf))
					}
					if len(p.vhBuf) > 2 && ses.protoVersion == 5 && p.vhBuf[2] != 0 {
						log.WithFields(log.Fields{
							"ClientId":    ses.clientId,
							"Reason Code": p.vhBuf[2],
						}).Debug("PUBCOMP received")
					}
				case model.CONNECT:
					pnLen := binary.BigEndian.Uint16(p.vhBuf)
					ses.protoVersion = p.vhBuf[2+pnLen]

					if !bytes.Equal(protoVersionToName[ses.protoVersion], p.vhBuf[2:2+pnLen]) { // [MQTT-3.1.2-1]
						if ses.protoVersion < 5 {
							ses.sendConnackFail(1) // [MQTT-3.1.2-2]
						} else {
							ses.sendConnackFail(132)
						}

						return errors.New("unsupported client protocol. Must be MQTT v3.1 (3), v3.1.1 (4) or v5 (5)")
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

					ses.connectFlags = p.vhBuf[3+pnLen]
					if ses.connectFlags&0x01 > 0 { // [MQTT-3.1.2-3]
						return errors.New("malformed CONNECT: reserved header flag must be 0")
					}

					// [MQTT-3.1.2-24]
					ses.keepAlive = time.Duration(binary.BigEndian.Uint16(p.vhBuf[4+pnLen:])) * time.Second * 3 / 2
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
				} else if ses.protoVersion == 5 {
					p.lenMul = 1
					ses.rxState = propertiesLen
				} else {
					ses.rxState = payload
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
		case propertiesLen:
			p.vhPropertyLen += uint32(rx[i]&127) * p.lenMul
			if p.lenMul *= 128; p.lenMul > maxVarLenMul {
				return errors.New("malformed packet: bad Properties Length")
			}

			if rx[i]&128 == 0 {
				if p.vhPropertyLen == 0 {
					switch p.controlType {
					case model.PUBLISH:
						ses.rxState = payload
					case model.PUBACK:
						ses.client.qos1Done(binary.BigEndian.Uint16(p.vhBuf))
						ses.rxState = controlAndFlags
					case model.PUBREC:
						if err := ses.handlePubrec(); err != nil {
							return err
						}
						ses.rxState = controlAndFlags
					case model.PUBREL:
						if err := ses.handlePubrel(); err != nil {
							return err
						}
						ses.rxState = controlAndFlags
					case model.PUBCOMP:
						ses.client.qos2Part2Done(binary.BigEndian.Uint16(p.vhBuf))
						ses.rxState = controlAndFlags
					default: // subscribe, unsubscribe, connect
						ses.rxState = payload
					}
				} else {
					switch p.controlType {
					case model.PUBLISH:
						ses.rxState = propertiesPUBLISH
					case model.PUBACK:
						ses.rxState = propertiesPUBACK
					case model.PUBREC:
						ses.rxState = propertiesPUBREC
					case model.PUBREL:
						ses.rxState = propertiesPUBREL
					case model.PUBCOMP:
						ses.rxState = propertiesPUBCOMP
					case model.SUBSCRIBE:
						p.vhBuf = p.vhBuf[:0]
						ses.rxState = propertiesSUBSCRIBE
					case model.UNSUBSCRIBE:
						p.vhBuf = p.vhBuf[:0]
						ses.rxState = propertiesUNSUBSCRIBE
					case model.CONNECT:
						p.vhBuf = p.vhBuf[:0]
						ses.rxState = propertiesCONNECT
					}
				}
			}

			p.remainingLength--
			i++
		case propertiesPUBLISH:
			toRead := p.vhPropertyLen
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vhBuf = append(p.vhBuf, rx[i:i+toRead]...)
			p.remainingLength -= toRead
			p.vhPropertyLen -= toRead

			if p.vhPropertyLen == 0 {
				if err := s.handlePublishProperties(ses); err != nil {
					return err
				}
				ses.rxState = payload
			}

			i += toRead
		case propertiesPUBACK:
			toRead := p.vhPropertyLen
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vhBuf = append(p.vhBuf, rx[i:i+toRead]...)
			p.remainingLength -= toRead
			p.vhPropertyLen -= toRead

			if p.vhPropertyLen == 0 {
				// TODO: handle PUBACK props
				ses.client.qos1Done(binary.BigEndian.Uint16(p.vhBuf))
				ses.rxState = controlAndFlags
			}

			i += toRead
		case propertiesPUBREC:
			toRead := p.vhPropertyLen
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vhBuf = append(p.vhBuf, rx[i:i+toRead]...)
			p.remainingLength -= toRead
			p.vhPropertyLen -= toRead

			if p.vhPropertyLen == 0 {
				// TODO: handle PUBREC props
				if err := ses.handlePubrec(); err != nil {
					return err
				}
				ses.rxState = controlAndFlags
			}

			i += toRead
		case propertiesPUBREL:
			toRead := p.vhPropertyLen
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vhBuf = append(p.vhBuf, rx[i:i+toRead]...)
			p.remainingLength -= toRead
			p.vhPropertyLen -= toRead

			if p.vhPropertyLen == 0 {
				// TODO: handle PUBREL props
				if err := ses.handlePubrel(); err != nil {
					return err
				}
				ses.rxState = controlAndFlags
			}

			i += toRead
		case propertiesPUBCOMP:
			toRead := p.vhPropertyLen
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vhBuf = append(p.vhBuf, rx[i:i+toRead]...)
			p.remainingLength -= toRead
			p.vhPropertyLen -= toRead

			if p.vhPropertyLen == 0 {
				// TODO: handle PUBCOMP props
				ses.client.qos2Part2Done(binary.BigEndian.Uint16(p.vhBuf))
				ses.rxState = controlAndFlags
			}

			i += toRead
		case propertiesSUBSCRIBE:
			toRead := p.vhPropertyLen
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vhBuf = append(p.vhBuf, rx[i:i+toRead]...)
			p.remainingLength -= toRead
			p.vhPropertyLen -= toRead

			if p.vhPropertyLen == 0 {
				if err := s.handleSubscribeProperties(ses); err != nil {
					return err
				}
				ses.rxState = payload
			}

			i += toRead
		case propertiesUNSUBSCRIBE:
			toRead := p.vhPropertyLen
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vhBuf = append(p.vhBuf, rx[i:i+toRead]...)
			p.remainingLength -= toRead
			p.vhPropertyLen -= toRead

			if p.vhPropertyLen == 0 {
				if err := s.handleUnSubscribeProperties(ses); err != nil {
					return err
				}
				ses.rxState = payload
			}

			i += toRead
		case propertiesCONNECT:
			toRead := p.vhPropertyLen
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vhBuf = append(p.vhBuf, rx[i:i+toRead]...)
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
			ses.sendConnackFail(2)
			return errors.New("invalid CONNECT: ClientId must be between 1-23 characters long for MQTT v3.1 (3)")
		}

		if err := checkUTF8(p[2:offs], false); err != nil { // [MQTT-3.1.3-4]
			return errors.New("malformed CONNECT: bad ClientId: " + err.Error())
		}

		ses.clientId = string(p[2:offs])
	} else {
		if ses.protoVersion < 5 && ses.persistent() { // [MQTT-3.1.3-7]
			ses.sendConnackFail(2) // [MQTT-3.1.3-8]
			return errors.New("malformed CONNECT: must have ClientId present when persistent session")
		}

		newUnNamed := atomic.AddUint32(&unNamedClients, 1)
		ses.clientId = fmt.Sprintf("noname-%d-%d", newUnNamed, time.Now().UnixNano()/100000)
		ses.assignedCId = true
	}

	// Will Properties, Will Topic & Will Message/Payload
	if ses.connectFlags&0x04 > 0 {
		// Properties
		if ses.protoVersion == 5 {
			len, lenMul := 0, 1
			for done := false; !done; offs++ {
				len += int(p[offs]&127) * lenMul
				if lenMul *= 128; lenMul > maxVarLenMul {
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

	sessionIsPresent := s.addSession(ses)
	ses.stopped.Add(1)
	go ses.startWriter()

	// [MQTT-3.2.2-1, 2-2, 2-3]
	ses.sendConnackSuccess(sessionIsPresent)

	ses.connectSent = true
	ses.run()
	return nil
}

func (s *Server) handleConnectProperties(ses *session) error {
	return nil // TODO: store and use Connect Properties
}

func (s *Server) handlePublish(ses *session) error {
	p := &ses.packet
	topicLen := binary.BigEndian.Uint16(p.vhBuf)
	pub := model.NewPub(p.flags, p.vhBuf[:topicLen+2], p.payload)

	if log.IsLevelEnabled(log.DebugLevel) {
		lf := log.Fields{
			"ClientId":   ses.clientId,
			"Topic Name": string(p.vhBuf[2 : topicLen+2]),
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

	switch pub.RxQoS() {
	case 0:
		s.pubs.Add(queue.GetItem(pub))
	case 1:
		s.pubs.Add(queue.GetItem(pub))
		return ses.sendPuback(p.pID)
	case 2: // [MQTT-4.3.3-2]
		if _, ok := ses.client.q2RxLookup[p.pID]; !ok {
			ses.client.q2RxLookup[p.pID] = struct{}{}
			s.pubs.Add(queue.GetItem(pub))
		}
		return ses.sendPubrec(p.pID)
	}

	return nil
}

func (s *Server) handlePublishProperties(ses *session) error {
	return nil // TODO: store and use Publish Properties
}

func (s *Server) handleSubscribe(ses *session) error {
	p := ses.packet.payload
	topics, qoss := make([][][]byte, 0, 2), make([]uint8, 0, 2)
	i := uint32(0)

	for i < uint32(len(p)) {
		topicL := uint32(binary.BigEndian.Uint16(p[i:]))
		i += 2
		topicEnd := i + topicL

		if ses.protoVersion == 5 {
			if p[topicEnd]&0xC0 != 0 { // v5[MQTT-3.8.3-5]
				return errors.New("malformed SUBSCRIBE: reserved bits in payload must be 0")
			}
			// TODO: store and use No Local, Retain As Published & Retain Handling options
		} else if p[topicEnd]&0xFC != 0 { // [MQTT-3-8.3-4]
			return errors.New("malformed SUBSCRIBE: reserved bits in payload must be 0")
		}

		topic := p[i:topicEnd]
		if err := checkUTF8(topic, false); err != nil { // [MQTT-3.8.3-1]
			return errors.New("malformed SUBSCRIBE: bad Topic Filter: " + err.Error())
		}

		if log.IsLevelEnabled(log.DebugLevel) {
			log.WithFields(log.Fields{
				"ClientId":     ses.clientId,
				"Topic Filter": string(topic),
				"QoS":          p[topicEnd],
			}).Debug("SUBSCRIBE received")
		}

		topics, qoss = append(topics, bytes.Split(topic, []byte{'/'})), append(qoss, p[topicEnd])
		i += 1 + topicL
	}

	s.addSubscriptions(ses.client, topics, qoss)

	// [MQTT-3.8.4-1, 4-4, 4-5, 4-6]
	return ses.sendSuback(qoss)
}

func (s *Server) handleSubscribeProperties(ses *session) error {
	return nil // TODO: store and use Subscribe Properties
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
	return ses.sendUnsuback(len(topics))
}

func (s *Server) handleUnSubscribeProperties(ses *session) error {
	return nil // TODO: store and use Unsubscribe Properties
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
