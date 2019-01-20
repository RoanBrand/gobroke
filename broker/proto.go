package broker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

func (s *server) parseStream(ses *session, rx []byte) error {
	p := &ses.packet
	l := uint32(len(rx))
	var i uint32

	for i < l {
		switch ses.rxState {
		case controlAndFlags:
			p.controlType = rx[i] & 0xF0
			p.flags = rx[i] & 0x0F
			if p.controlType < CONNECT || p.controlType > DISCONNECT {
				return fmt.Errorf("invalid control packet type: %d", p.controlType)
			}

			// handle first and only connect
			if ses.sentConnectPacket {
				if p.controlType == CONNECT { // [MQTT-3.1.0-2]
					// protocol violation
					ses.close()
					return errors.New("client's second CONNECT packet")
				}
			} else {
				if p.controlType != CONNECT { // [MQTT-3.1.0-1]
					ses.close()
					return errors.New("client must send CONNECT packet first")
				}
			}

			switch p.controlType {
			case PUBLISH:
				if (p.flags&0x08 > 0) && (p.flags&0x06 == 0) { // [MQTT-3.3.1-2]
					ses.close()
					return errors.New("malformed PUBLISH")
				}
				if p.flags&0x06 == 6 { // [MQTT-3.3.1-4]
					ses.close()
					return errors.New("malformed PUBLISH")
				}
				break
			case SUBSCRIBE:
				if p.flags != 0x02 { // [MQTT-3.8.1-1]
					ses.close()
					return errors.New("malformed SUBSCRIBE")
				}
			case DISCONNECT:
				log.WithFields(log.Fields{
					"id": ses.clientId,
				}).Debug("Got DISCONNECT packet")
			}

			p.lenMul = 1
			p.remainingLength = 0
			ses.rxState = length
			i++
		case length:
			p.remainingLength += uint32(rx[i]&127) * p.lenMul
			p.lenMul *= 128
			if p.lenMul > 128*128*128 {
				return errors.New("malformed remaining length")
			}

			if rx[i]&128 == 0 {
				switch p.controlType {
				case CONNECT:
					p.vhLen = 10
				case PUBLISH:
					p.vhLen = 0 // will increase later
					p.gotVhLen = false
				case PUBACK:
					p.vhLen = 2
				case SUBSCRIBE:
					if p.remainingLength < 5 { // [MQTT-3.8.3-3]
						ses.close()
						return errors.New("invalid SUBSCRIBE")
					}
					p.vhLen = 2
				case PINGREQ:
					go ses.writePacket(pingRespPacket)
				}

				if p.remainingLength == 0 {
					ses.setDeadline()
					ses.rxState = controlAndFlags
				} else {
					p.vh = p.vh[:0]
					ses.rxState = variableHeader
				}
			}

			i++
		case variableHeader:
			switch p.controlType {
			case CONNECT:
				avail := l - i
				toRead := p.vhLen
				if avail < toRead {
					toRead = avail
				}

				p.vh = append(p.vh, rx[i:i+toRead]...)
				p.remainingLength -= toRead
				p.vhLen -= toRead

				if p.vhLen == 0 {
					if !bytes.Equal(connectPacket, p.vh[:7]) { // [MQTT-3.1.2-1]
						ses.sendConnack(1) // [MQTT-3.1.2-2]
						ses.close()
						return errors.New("bad CONNECT: invalid protocol")
					}

					ses.connectFlags = p.vh[7]
					if ses.connectFlags&0x01 > 0 { // [MQTT-3.1.2-3]
						ses.close()
						return errors.New("bad CONNECT")
					}

					// [MQTT-3.1.2-24]
					ses.keepAlive = time.Duration(binary.BigEndian.Uint16(p.vh[8:])) * time.Second * 3 / 2
					p.payload = p.payload[:0]
					ses.rxState = payload
				}

				i += toRead
			case PUBLISH:
				if !p.gotVhLen {
					p.vh = append(p.vh, rx[i])
					p.vhLen++
					p.remainingLength--

					if p.vhLen == 2 {
						// vhLen is now remaining
						p.vhLen = uint32(binary.BigEndian.Uint16(p.vh))
						if p.flags&0x06 > 0 { // Qos > 0
							p.vhLen += 2
						}
						p.gotVhLen = true
					}

					i++
				} else {
					avail := l - i
					toRead := p.vhLen
					if avail < toRead {
						toRead = avail
					}

					p.vh = append(p.vh, rx[i:i+toRead]...)
					p.remainingLength -= toRead
					p.vhLen -= toRead

					if p.vhLen == 0 {
						if p.flags&0x06 > 0 { // Qos > 0
							p.pID = binary.BigEndian.Uint16(p.vh[len(p.vh)-2:])
						}
						p.payload = p.payload[:0]
						ses.rxState = payload
					}

					i += toRead
				}
			case PUBACK:
				avail := l - i
				toRead := p.vhLen
				if avail < toRead {
					toRead = avail
				}

				p.vh = append(p.vh, rx[i:i+toRead]...)
				p.remainingLength -= toRead
				p.vhLen -= toRead

				if p.vhLen == 0 {
					go ses.signalQoS1Done(binary.BigEndian.Uint16(p.vh))
				}

				i += toRead
			case SUBSCRIBE:
				avail := l - i
				toRead := p.vhLen
				if avail < toRead {
					toRead = avail
				}

				p.vh = append(p.vh, rx[i:i+toRead]...)
				p.remainingLength -= toRead
				p.vhLen -= toRead

				if p.vhLen == 0 {
					p.payload = p.payload[:0]
					ses.rxState = payload
				}

				i += toRead
			default:
				// TODO: handle other variable headers
				p.remainingLength--
				i++
			}

			if p.remainingLength == 0 {
				ses.setDeadline()
				ses.rxState = controlAndFlags
			}

		case payload:
			avail := l - i
			toRead := p.remainingLength
			if avail < toRead {
				toRead = avail
			}

			p.payload = append(p.payload, rx[i:i+toRead]...)
			p.remainingLength -= toRead

			if p.remainingLength == 0 {
				switch p.controlType {
				case CONNECT:
					s.handleConnect(ses)
				case PUBLISH:
					s.handlePublish(ses)
				case SUBSCRIBE:
					s.handleSubscribe(ses)
				}
				ses.setDeadline()
				ses.rxState = controlAndFlags
			}

			i += toRead
		}
	}

	return nil
}

func (s *server) handleSubscribe(ses *session) {
	p := ses.packet.payload
	topics := make([]string, 0, 2)
	qoss := make([]uint8, 0, 2)
	i := 0

	for i < len(p) {
		topicL := int(binary.BigEndian.Uint16(p[i:]))
		topicEnd := i + 2 + topicL
		topics = append(topics, string(p[i+2:topicEnd]))
		if p[topicEnd]&0xFC != 0 { // [MQTT-3-8.3-4]
			ses.close()
			log.Println("invalid subscribe QoS byte")
			return
		}
		if p[topicEnd] == 2 {
			p[topicEnd] = 1 // Grant max 1 for now. TODO: Support QoS 2
		}
		qoss = append(qoss, p[topicEnd])

		i += 3 + topicL
	}

	log.WithFields(log.Fields{
		"id":     ses.clientId,
		"Topics": topics,
	}).Debug("Got SUBSCRIBE packet")

	go s.addSubscriptions(topics, qoss, ses.clientId)

	// [MQTT-3.8.4-1, 4-4, 4-5, 4-6]
	tl := len(topics)
	subackP := make([]byte, 1, tl+7)
	subackP[0] = SUBACK
	subackP = append(subackP, variableLengthEncode(tl+2)...)
	subackP = append(subackP, ses.packet.vh[0], ses.packet.vh[1]) // [MQTT-3.8.4-2]
	subackP = append(subackP, qoss...)                            // [MQTT-3.9.3-1]

	go ses.writePacket(subackP)
}

func (s *server) handlePublish(ses *session) {
	p := &ses.packet
	s.publishToSubscribers(p, ses.clientId)

	switch p.flags & 0x06 {
	case 0x02: // QoS 1
		go ses.writePacket([]byte{PUBACK, 2, byte(p.pID >> 8), byte(p.pID)})
	case 0x04: // QoS 2
	}
}

func variableLengthEncode(l int) []byte {
	res := make([]byte, 0, 2)
	for {
		eb := l % 128
		l = l / 128
		if l > 0 {
			eb = eb | 128
		}
		res = append(res, byte(eb))
		if l <= 0 {
			break
		}
	}
	return res
}

var unNamedClients int

func (s *server) handleConnect(ses *session) {
	p := ses.packet.payload
	if len(p) < 2 { // [MQTT-3.1.3-3]
		ses.close() // close conn. throw all away
		log.Println("CONNECT packet payload error")
		return
	}

	clientIdLen := binary.BigEndian.Uint16(p)
	if clientIdLen > 0 {
		ses.clientId = string(p[2 : clientIdLen+2])
	} else {
		if ses.persistent() { // [MQTT-3.1.3-7]
			ses.sendConnack(2) // [MQTT-3.1.3-8]
			ses.close()
			log.Println("CONNECT packet wrong")
			return
		}

		ses.clientId = fmt.Sprintf("noname-%d-%d", unNamedClients, time.Now().UnixNano()/100000)
		unNamedClients++
	}

	// TODO: Will Topic, Will Msg, User Name, Password

	ses.sentConnectPacket = true
	ses.setDeadline()
	s.addClient(ses)
	ses.sendConnack(0)
	go ses.startWriter()
}

func (s *server) handleNewConn(conn net.Conn) {
	// TODO: Do not spin up session unless CONNECT received first, and in timely fashion.
	newSession := session{
		conn:          conn,
		tx:            bufio.NewWriter(conn),
		txFlush:       make(chan struct{}, 1),
		subscriptions: make(map[string]struct{}),
		qos1Queue:     make(map[uint16]chan<- struct{}, 2),
	}
	newSession.packet.vh = make([]byte, 0, 512)
	newSession.packet.vh = make([]byte, 0, 512)

	rx := make([]byte, 1024)
	for {
		n, err := conn.Read(rx)
		if err != nil {
			if err.Error() == "EOF" {
				log.Println("client closed connection gracefully")
				if !newSession.persistent() { // [MQTT-3.1.2-6]
					s.removeClient(newSession.clientId)
				}
				return
			}

			if strings.Contains(err.Error(), "use of closed") {
				return
			}

			log.Println("error tcp rx, closing conn:", err)
			newSession.close()
			return
		}

		if n > 0 {
			if err := s.parseStream(&newSession, rx[:n]); err != nil {
				log.Println("error parsing rx stream:", err)
				return
			}
		}
	}
}
