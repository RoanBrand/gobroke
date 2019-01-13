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

func (s *server) parseStream(session *session, rx []byte) error {
	p := &session.packet
	l := uint32(len(rx))
	var i uint32

	for i < l {
		switch session.rxState {
		case controlAndFlags:
			p.controlType = rx[i] & 0xF0
			p.flags = rx[i] & 0x0F
			if p.controlType < CONNECT || p.controlType > DISCONNECT {
				return fmt.Errorf("invalid control packet type: %d", p.controlType)
			}

			if p.controlType == DISCONNECT {
				log.WithFields(log.Fields{
					"id": session.clientId,
				}).Debug("Got DISCONNECT packet")
			}

			if p.controlType == SUBSCRIBE && p.flags != 0x02 { // [MQTT-3.8.1-1]
				session.close()
				return errors.New("malformed SUBSCRIBE")
			}

			// handle first and only connect
			if session.sentConnectPacket {
				if p.controlType == CONNECT { // [MQTT-3.1.0-2]
					// protocol violation
					session.close()
					return errors.New("client's second CONNECT packet")
				}
			} else {
				if p.controlType != CONNECT { // [MQTT-3.1.0-1]
					session.close()
					return errors.New("client must send CONNECT packet first")
				}
			}

			p.lenMul = 1
			p.remainingLength = 0
			session.rxState = length
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
				case SUBSCRIBE:
					if p.remainingLength < 5 { // [MQTT-3.8.3-3]
						session.close()
						return errors.New("invalid SUBSCRIBE")
					}
					p.vhLen = 2
				case PINGREQ:
					session.writePacket(pingRespPacket)
				}

				if p.remainingLength == 0 {
					session.setDeadline()
					session.rxState = controlAndFlags
				} else {
					p.variableHeader = p.variableHeader[:0]
					session.rxState = variableHeader
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

				p.variableHeader = append(p.variableHeader, rx[i:i+toRead]...)
				p.remainingLength -= toRead
				p.vhLen -= toRead

				if p.vhLen == 0 {
					if !bytes.Equal(connectPacket, p.variableHeader[:7]) { // [MQTT-3.1.2-1]
						session.sendConnack(1) // [MQTT-3.1.2-2]
						session.close()
						return errors.New("bad CONNECT: invalid protocol")
					}

					session.connectFlags = p.variableHeader[7]
					if session.connectFlags&0x01 > 0 { // [MQTT-3.1.2-3]
						session.close()
						return errors.New("bad CONNECT")
					}

					// [MQTT-3.1.2-24]
					session.keepAlive = time.Duration(binary.BigEndian.Uint16(p.variableHeader[8:])) * time.Second * 3 / 2
					p.payload = p.payload[:0]
					session.rxState = payload
				}

				i += toRead
			case PUBLISH:
				if !p.gotVhLen {
					p.variableHeader = append(p.variableHeader, rx[i])
					p.vhLen++
					p.remainingLength--

					if p.vhLen == 2 {
						// vhLen is now remaining
						p.vhLen = uint32(binary.BigEndian.Uint16(p.variableHeader))
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

					p.variableHeader = append(p.variableHeader, rx[i:i+toRead]...)
					p.remainingLength -= toRead
					p.vhLen -= toRead

					if p.vhLen == 0 {
						p.payload = p.payload[:0]
						session.rxState = payload
					}

					i += toRead
				}
			case SUBSCRIBE:
				avail := l - i
				toRead := p.vhLen
				if avail < toRead {
					toRead = avail
				}

				p.variableHeader = append(p.variableHeader, rx[i:i+toRead]...)
				p.remainingLength -= toRead
				p.vhLen -= toRead

				if p.vhLen == 0 {
					session.packet.identifier = binary.BigEndian.Uint16(p.variableHeader)
					p.payload = p.payload[:0]
					session.rxState = payload
				}

				i += toRead
			default:
				// TODO: handle other variable headers
				p.remainingLength--
				i++
			}

			if p.remainingLength == 0 {
				session.setDeadline()
				session.rxState = controlAndFlags
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
					s.handleConnect(session)
				case PUBLISH:
					s.handlePublish(session)
				case SUBSCRIBE:
					s.handleSubscribe(session)
				}
				session.setDeadline()
				session.rxState = controlAndFlags
			}

			i += toRead
		}
	}

	return nil
}

func (s *server) handleSubscribe(session *session) {
	p := session.packet.payload
	topics := make([]string, 0, 2)
	qoss := make([]uint8, 0, 2)
	i := 0

	for i < len(p) {
		topicL := int(binary.BigEndian.Uint16(p[i:]))
		topicEnd := i + 2 + topicL
		topics = append(topics, string(p[i+2:topicEnd]))
		if p[topicEnd]&0xFC != 0 { // [MQTT-3-8.3-4]
			session.close()
			log.Println("invalid subscribe QoS byte")
			return
		}
		qoss = append(qoss, p[topicEnd])

		i += 3 + topicL
	}

	log.WithFields(log.Fields{
		"id":     session.clientId,
		"Topics": topics,
	}).Debug("Got SUBSCRIBE packet")

	s.sLock.Lock()
	for i, t := range topics {
		cl, ok := s.subscriptions[t]
		if !ok {
			s.subscriptions[t] = make(map[string]uint8, 8)
			cl = s.subscriptions[t]
		}

		cl[session.clientId] = qoss[i]
	}
	s.sLock.Unlock()

	id := session.packet.identifier
	session.writePacket([]byte{SUBACK, 3, uint8(id >> 8), uint8(id), 0})
}

func (s *server) handlePublish(session *session) {
	p := &session.packet
	vh := p.variableHeader
	msg := p.payload
	mLen := len(msg)
	topicLen := int(binary.BigEndian.Uint16(vh))
	topic := string(vh[2 : topicLen+2])

	/*if p.flags&0x06 > 0 { // Qos > 0
		// uint16(vh[len(vh)-2]<<8) | uint16(vh[len(vh)-1])
		p.identifier = binary.BigEndian.Uint16(vh[len(vh)-2:])
	}*/

	log.WithFields(log.Fields{
		"id":      session.clientId,
		"Topic":   topic,
		"Payload": string(msg),
	}).Debug("Got PUBLISH packet")

	pubPack := make([]byte, 1, 7+topicLen+mLen) // ctrl 1 + remainLen 4max + topicLen 2 + topicLen + msgLen
	pubPack[0] = PUBLISH
	pubPack = append(pubPack, variableLengthEncode(topicLen+mLen+2)...)
	pubPack = append(pubPack, vh[:topicLen+2]...) // 2 bytes + topic
	pubPack = append(pubPack, msg...)

	go func(p []byte, topic string) {
		s.sLock.RLock()
		s.cLock.RLock()
		for c := range s.subscriptions[topic] {
			client, ok := s.clients[c]
			if ok {
				client.writePacket(p)
			}
		}
		s.sLock.RUnlock()
		s.cLock.RUnlock()
	}(pubPack, topic)
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

func (s *server) handleConnect(session *session) {
	p := session.packet.payload
	if len(p) < 2 { // [MQTT-3.1.3-3]
		session.close() // close conn. throw all away
		log.Println("CONNECT packet payload error")
		return
	}

	clientIdLen := binary.BigEndian.Uint16(p)
	if clientIdLen > 0 {
		session.clientId = string(p[2 : clientIdLen+2])
	} else {
		if session.persistent() { // [MQTT-3.1.3-7]
			session.sendConnack(2) // [MQTT-3.1.3-8]
			session.close()
			log.Println("CONNECT packet wrong")
			return
		}

		session.clientId = fmt.Sprintf("noname-%d-%d", unNamedClients, time.Now().UnixNano()/100000)
		unNamedClients++
	}

	// TODO: Will Topic, Will Msg, User Name, Password

	session.sentConnectPacket = true
	session.setDeadline()
	s.addClient(session)
	session.sendConnack(0)
	go session.startWriter()
}

func (s *server) handleNewConn(conn net.Conn) {
	// TODO: Do not spin up session unless CONNECT received first, and in timely fashion.
	newSession := session{
		conn:          conn,
		tx:            bufio.NewWriter(conn),
		txFlush:       make(chan struct{}, 1),
		subscriptions: make(map[string]struct{}),
		packet: packet{
			variableHeader: make([]byte, 0, 512),
			payload:        make([]byte, 0, 512),
		},
	}

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
