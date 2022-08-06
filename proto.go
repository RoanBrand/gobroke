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

	propertiesLen
	properties

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

	// Close the connection normally. Do not send the Will Message.
	errGotNormalDiscon = errors.New("Normal")
	// The Client wishes to disconnect but requires
	// that the Server also publishes its Will Message.
	errGotDisconWithWill = errors.New("WithWill")
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
				if p.flags != 0 { // [MQTT-3.14.1-1]
					ses.disconnectReasonCode = model.MalformedPacket
					return errors.New("malformed DISCONNECT: fixed header flags must be 0 (reserved)")
				}
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
				case model.PUBLISH:
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
				case model.CONNECT:
					p.vhToRead = 2
					p.vhBuf = p.vhBuf[:0]
					ses.rxState = variableHeaderLen
				case model.DISCONNECT:
					if p.remainingLength == 0 {
						log.WithFields(log.Fields{
							"ClientId": ses.clientId,
							"Reason":   "Normal disconnection",
						}).Debug("DISCONNECT received")

						return errGotNormalDiscon
					} else if ses.protoVersion < 5 {
						return errors.New("malformed DISCONNECT: proto < v5")
					}

					p.vhToRead = 1 // Reason Code
					p.vhBuf = p.vhBuf[:0]
					ses.rxState = variableHeader
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
							ses.disconnectReasonCode = model.MalformedPacket
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
							ses.sendConnackFail(model.UnsupportedProtocolVersion)
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
				case model.DISCONNECT:
					if p.remainingLength == 0 {
						return ses.handleDisconnect()
					}
				}

				//p.payload = p.payload[:0]
				if p.remainingLength == 0 {
					ses.updateTimeout()
					if p.controlType == model.PUBLISH {
						p.payload = p.payload[:0]
						if err := s.handlePublish(ses); err != nil {
							return err
						}
					}
					ses.rxState = controlAndFlags
				} else if ses.protoVersion > 4 {
					p.lenMul = 1
					ses.rxState = propertiesLen
				} else {
					p.payload = p.payload[:0]
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
			p.vhPropToRead += uint32(rx[i]&127) * p.lenMul
			if p.lenMul *= 128; p.lenMul > maxVarLenMul {
				return errors.New("malformed packet: bad Properties Length")
			}

			if rx[i]&128 == 0 {
				if p.vhPropToRead == 0 {
					switch p.controlType {
					case model.PUBLISH:
						p.payload = p.payload[:0]
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
					case model.DISCONNECT:
						return ses.handleDisconnect()
					default: // subscribe, unsubscribe, connect
						p.payload = p.payload[:0]
						ses.rxState = payload
					}
				} else {
					ses.rxState = properties
				}
			}

			p.remainingLength--
			i++
		case properties:
			toRead := p.vhPropToRead
			if avail := l - i; avail < toRead {
				toRead = avail
			}

			p.vhBuf = append(p.vhBuf, rx[i:i+toRead]...)
			p.remainingLength -= toRead
			p.vhPropToRead -= toRead

			if p.vhPropToRead == 0 {
				switch p.controlType {
				case model.PUBLISH:
					if err := s.handlePublishProperties(ses); err != nil {
						return err
					}
					p.payload = p.payload[:0]
					ses.rxState = payload
				case model.PUBACK:
					// TODO: handle PUBACK props
					ses.client.qos1Done(binary.BigEndian.Uint16(p.vhBuf))
					ses.rxState = controlAndFlags
				case model.PUBREC:
					// TODO: handle PUBREC props
					if err := ses.handlePubrec(); err != nil {
						return err
					}
					ses.rxState = controlAndFlags
				case model.PUBREL:
					// TODO: handle PUBREL props
					if err := ses.handlePubrel(); err != nil {
						return err
					}
					ses.rxState = controlAndFlags
				case model.PUBCOMP:
					// TODO: handle PUBCOMP props
					ses.client.qos2Part2Done(binary.BigEndian.Uint16(p.vhBuf))
					ses.rxState = controlAndFlags
				case model.SUBSCRIBE:
					if err := s.handleSubscribeProperties(ses); err != nil {
						return err
					}
					p.payload = p.payload[:0]
					ses.rxState = payload
				case model.UNSUBSCRIBE:
					if err := s.handleUnSubscribeProperties(ses); err != nil {
						return err
					}
					p.payload = p.payload[:0]
					ses.rxState = payload
				case model.CONNECT:
					if err := s.handleConnectProperties(ses); err != nil {
						return err
					}
					p.payload = p.payload[:0]
					ses.rxState = payload
				case model.DISCONNECT:
					return s.handleDisconnectProperties(ses)
				}
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
		if ses.protoVersion < 5 && !ses.cleanStart() { // [MQTT-3.1.3-7]
			ses.sendConnackFail(2) // [MQTT-3.1.3-8]
			return errors.New("malformed CONNECT: must have ClientId with CleanSession set to 0")
		}

		newUnNamed := atomic.AddUint32(&unNamedClients, 1)
		ses.clientId = fmt.Sprintf("noname-%d-%d", newUnNamed, time.Now().UnixNano()/100000)
		ses.assignedCId = true
	}

	// Will Properties, Will Topic & Will Message/Payload
	if ses.connectFlags&0x04 > 0 {
		// Properties
		if ses.protoVersion == 5 {
			wpLen, vbLen, err := variableLengthDecode(p[offs:])
			if err != nil {
				return errors.New("malformed CONNECT: bad Will Properties Length")
			}
			// TODO: store and use Will Properties
			offs += uint32(vbLen) + uint32(wpLen)
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
		return errors.New("malformed CONNECT: unexpected extra payload fields (Will Properties, Topic, Message, or User Name or Password)")
	}

	if err := ses.conn.SetReadDeadline(time.Time{}); err != nil { // CONNECT packet timeout cancel
		return err
	}

	ses.connectSent = true
	sessionIsPresent := s.addSession(ses)
	ses.ended.Add(1)
	go ses.startWriter()

	// [MQTT-3.2.2-1, 2-2, 2-3]
	ses.sendConnackSuccess(sessionIsPresent)
	ses.run()
	return nil
}

func (s *Server) handleConnectProperties(ses *session) error {
	vh := ses.packet.vhBuf
	pnLen := binary.BigEndian.Uint16(vh)
	props := vh[pnLen+6:]
	gotSesExp, gotRxMax, gotMaxPSize, gotTopAlias, gotRRI, gotRPI := false, false, false, false, false, false

	for i := 0; i < len(props); {
		remain := len(props) - i
		switch props[i] {
		case model.SessionExpiryInterval:
			if gotSesExp {
				return errors.New("malformed CONNECT: Session Expiry Interval included more than once")
			}
			if remain < 5 {
				return errors.New("malformed CONNECT: bad Session Expiry Interval")
			}

			ses.expiryInterval = binary.BigEndian.Uint32(props[i+1:])
			gotSesExp = true
			i += 5
		case model.ReceiveMaximum:
			if gotRxMax {
				return errors.New("malformed CONNECT: Receive Maximum included more than once")
			}
			if remain < 3 {
				return errors.New("malformed CONNECT: bad Receive Maximum")
			}

			ses.receiveMax = binary.BigEndian.Uint16(props[i+1:])
			if ses.receiveMax == 0 {
				return errors.New("malformed CONNECT: Receive Maximum 0 not allowed")
			}

			gotRxMax = true
			i += 3
		case model.MaximumPacketSize:
			if gotMaxPSize {
				return errors.New("malformed CONNECT: Maximum Packet Size included more than once")
			}
			if remain < 5 {
				return errors.New("malformed CONNECT: bad Maximum Packet Size")
			}

			ses.maxPacketSize = binary.BigEndian.Uint32(props[i+1:])
			if ses.maxPacketSize == 0 {
				return errors.New("malformed CONNECT: Maximum Packet Size 0 not allowed")
			}

			gotMaxPSize = true
			i += 5
		case model.TopicAliasMaximum:
			if gotTopAlias {
				return errors.New("malformed CONNECT: Topic Alias Maximum included more than once")
			}
			if remain < 3 {
				return errors.New("malformed CONNECT: bad Topic Alias Maximum")
			}

			ses.topicAliasMax = binary.BigEndian.Uint16(props[i+1:])
			gotTopAlias = true
			i += 3
		case model.RequestResponseInformation:
			if gotRRI {
				return errors.New("malformed CONNECT: Request Response Information included more than once")
			}
			if remain < 1 {
				return errors.New("malformed CONNECT: bad Request Response Information")
			}
			if props[i+1] != 0 && props[i+1] != 1 {
				return errors.New("malformed CONNECT: bad Request Response Information")
			}

			gotRRI = true
			i += 2
		case model.RequestProblemInformation:
			if gotRPI {
				return errors.New("malformed CONNECT: Request Problem Information included more than once")
			}
			if remain < 1 {
				return errors.New("malformed CONNECT: bad Request Problem Information")
			}
			if props[i+1] != 0 && props[i+1] != 1 {
				return errors.New("malformed CONNECT: bad Request Problem Information")
			}

			ses.reqProblemInfo = props[i+1] == 1
			gotRPI = true
			i += 2
		case model.UserProperty:
			if remain < 5 {
				return errors.New("malformed CONNECT: bad User Property")
			}

			kLen := int(binary.BigEndian.Uint16(props[i+1:]))
			if remain < 5+kLen {
				return errors.New("malformed CONNECT: bad User Property")
			}

			vLen := int(binary.BigEndian.Uint16(props[i+3+kLen:]))
			expect := 5 + kLen + vLen
			if remain < expect {
				return errors.New("malformed CONNECT: bad User Property")
			}

			i += expect
		case model.AuthenticationMethod:
			if remain < 3 {
				return errors.New("malformed CONNECT: bad Authentication Method Property")
			}

			l := int(binary.BigEndian.Uint16(props[i+1:]))
			if remain < 3+l {
				return errors.New("malformed CONNECT: bad Authentication Method Property")
			}

			i += 3 + l
		case model.AuthenticationData:
			if remain < 3 {
				return errors.New("malformed CONNECT: bad Authentication Data Property")
			}

			l := int(binary.BigEndian.Uint16(props[i+1:]))
			if remain < 3+l {
				return errors.New("malformed CONNECT: bad Authentication Data Property")
			}

			i += 3 + l
		default:
			return fmt.Errorf("malformed CONNECT: unknown property %d (0x%x)", props[i], props[i])
		}
	}
	return nil
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

		topics, qoss = append(topics, bytes.Split(topic, []byte{'/'})), append(qoss, p[topicEnd]&3)
		i += 1 + topicL
	}

	s.addSubscriptions(ses.client, topics, qoss)

	// [MQTT-3.8.4-1, 4-4, 4-5, 4-6]
	return ses.sendSuback(qoss)
}

func (s *Server) handleSubscribeProperties(ses *session) error {
	props := ses.packet.vhBuf[2:]
	//gotSubId := false

	for i := 0; i < len(props); {
		remain := len(props) - i
		switch props[i] {
		case model.SubscriptionIdentifier:
			ses.disconnectReasonCode = model.SubscriptionIdsNotSupported
			return errors.New("malformed SUBSCRIBE: Subscription Identifiers not supported")
			/*if gotSubId {
				ses.disconnectReasonCode = model.ProtocolError
				return errors.New("malformed SUBSCRIBE: Subscription Identifier included more than once")
			}
			if remain < 2 {
				return errors.New("malformed SUBSCRIBE: bad Subscription Identifier")
			}
			maxSubId, l, err := variableLengthDecode(props[i+1:])
			if err != nil {
				return errors.New("malformed SUBSCRIBE: bad Subscription Identifier")
			}
			if maxSubId == 0 {
				ses.disconnectReasonCode = model.ProtocolError
				return errors.New("malformed SUBSCRIBE: Subscription Identifier 0 not allowed")
			}

			gotSubId = true
			i += 1 + l*/
		case model.UserProperty:
			if remain < 5 {
				return errors.New("malformed SUBSCRIBE: bad User Property")
			}

			kLen := int(binary.BigEndian.Uint16(props[i+1:]))
			if remain < 5+kLen {
				return errors.New("malformed SUBSCRIBE: bad User Property")
			}

			vLen := int(binary.BigEndian.Uint16(props[i+3+kLen:]))
			expect := 5 + kLen + vLen
			if remain < expect {
				return errors.New("malformed SUBSCRIBE: bad User Property")
			}

			i += expect
		default:
			return fmt.Errorf("malformed SUBSCRIBE: unknown property %d (0x%x)", props[i], props[i])
		}
	}
	return nil
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

func (s *Server) handleDisconnectProperties(ses *session) error {
	props := ses.packet.vhBuf[1:]
	gotSesExp, gotReasonStr := false, false

	for i := 0; i < len(props); {
		remain := len(props) - i

		switch props[i] {
		case model.SessionExpiryInterval:
			if gotSesExp {
				return errors.New("malformed DISCONNECT: Session Expiry Interval included more than once")
			}
			if remain < 5 {
				return errors.New("malformed DISCONNECT: bad Session Expiry Interval")
			}

			newExp := binary.BigEndian.Uint32(props[i+1:])
			if ses.expiryInterval == 0 && newExp != 0 {
				ses.disconnectReasonCode = model.ProtocolError
				return errors.New("malformed DISCONNECT: bad Session Expiry Interval")
			}

			ses.expiryInterval = newExp
			gotSesExp = true
			i += 5
		case model.ReasonString:
			if gotReasonStr {
				return errors.New("malformed DISCONNECT: Reason String included more than once")
			}
			if remain < 3 {
				return errors.New("malformed DISCONNECT: bad Reason String")
			}

			l := int(binary.BigEndian.Uint16(props[i+1:]))
			if remain < 3+l {
				return errors.New("malformed DISCONNECT: bad Reason String")
			}

			gotReasonStr = true
			i += 3 + l
		case model.UserProperty:
			if remain < 5 {
				return errors.New("malformed DISCONNECT: bad User Property")
			}

			kLen := int(binary.BigEndian.Uint16(props[i+1:]))
			if remain < 5+kLen {
				return errors.New("malformed DISCONNECT: bad User Property")
			}

			vLen := int(binary.BigEndian.Uint16(props[i+3+kLen:]))
			expect := 5 + kLen + vLen
			if remain < expect {
				return errors.New("malformed DISCONNECT: bad User Property")
			}

			i += expect
		case model.ServerReference:
			return errors.New("malformed DISCONNECT: Server Reference should only be sent by Server")
		default:
			return fmt.Errorf("malformed DISCONNECT: unknown property %d (0x%x)", props[i], props[i])
		}
	}

	return ses.handleDisconnect()
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

func variableLengthDecode(b []byte) (int, int, error) {
	l, lMul, i := 0, 1, 0
	var err error

	for done := false; !done; i++ {
		if i > len(b)-1 {
			err = errors.New("bad Variable Length")
			break
		}

		l += int(b[i]&127) * lMul
		if lMul *= 128; lMul > maxVarLenMul {
			err = errors.New("bad Variable Length")
			break
		}

		done = b[i]&128 == 0
	}
	return l, i, err
}
