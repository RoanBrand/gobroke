// TODO: Websocket, Config, Qos1&2, path system, wildcards, $SYS topic, persistence
// TODO: rate limiting, TLS, user system with subscribe white- & blacklist

package go_mqtt_server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

type server struct {
	clients map[string]*session
	cLock   sync.RWMutex

	subscriptions map[string]map[string]uint8 // channel -> subscribed client id -> QoS
	sLock         sync.RWMutex
}

func NewServer() *server {
	return &server{
		clients:       make(map[string]*session, 16),
		subscriptions: make(map[string]map[string]uint8, 4),
	}
}

type session struct {
	conn             net.Conn
	serverClosedConn bool

	packet  packet
	rxState uint8
	tx      chan []byte
	rx      chan struct{}

	sentConnectPacket bool
	notFirstSession   bool
	connectFlags      byte
	keepAlive         uint16

	clientId      string
	subscriptions map[string]struct{} // unused. list of channels
	// QoS 1 & 2 unacknowledged and pending

}

func (s *session) close() {
	if !s.serverClosedConn {
		close(s.tx) // dont like this
	}
	s.serverClosedConn = true
	s.conn.Close()

}

func (s *session) sendConnack(errCode uint8) error {
	// [MQTT-3.2.2-1, 2-2, 2-3]
	var sp byte = 0
	if errCode == 0 && s.notFirstSession {
		sp = 1
	}
	p := []byte{CONNACK << 4, 2, sp, errCode}
	_, err := s.conn.Write(p)
	return err
}

func (s *session) sendInvalidProtocol() error {
	s.sendConnack(1) // [MQTT-3.1.2-2]
	s.close()
	return errors.New("bad CONNECT: invalid protocol")
}

func (s *session) stickySession() bool {
	return s.connectFlags&0x02 == 0
}

func (s *session) startWriter() {
	for p := range s.tx {
		if _, err := s.conn.Write(p); err != nil {
			log.Println("Error tcp tx:", err)
			return
		}
	}
}

func (s *session) watchDog() {
	per := time.Second * time.Duration(s.keepAlive) * 3 / 2
	t := time.NewTimer(per)
	for {
		select {
		case <-t.C:
			s.close() // [MQTT-3.1.2-24]
			return
		case <-s.rx:
			if !t.Stop() {
				<-t.C
			}
			t.Reset(per)
		}
	}
}

func (s *server) parseStream(session *session, rx []byte) error {
	p := &session.packet
	i := 0
	for i < len(rx) {
		switch session.rxState {
		case controlAndFlags:
			p.controlType = rx[i] >> 4
			p.flags = rx[i] & 0x0F
			if p.controlType < CONNECT || p.controlType > DISCONNECT {
				return fmt.Errorf("invalid control packet type: %d", p.controlType)
			}

			if p.controlType == DISCONNECT {
				log.Println("GOT DISCONNECT")
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
					session.tx <- []byte{PINGRESP << 4, 0}
				}

				if p.remainingLength == 0 {
					session.rx <- struct{}{}
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
				switch p.vhLen {
				case 10:
					if rx[i] != 0 {
						return session.sendInvalidProtocol()
					}
				case 9:
					if rx[i] != 4 {
						return session.sendInvalidProtocol()
					}
				case 8:
					if rx[i] != 'M' {
						return session.sendInvalidProtocol()
					}
				case 7:
					if rx[i] != 'Q' {
						return session.sendInvalidProtocol()
					}
				case 6:
					if rx[i] != 'T' {
						return session.sendInvalidProtocol()
					}
				case 5:
					if rx[i] != 'T' {
						return session.sendInvalidProtocol()
					}
				case 4:
					if rx[i] != 4 {
						return session.sendInvalidProtocol()
					}
				case 3:
					session.connectFlags = rx[i]
					if session.connectFlags&0x01 > 0 { // [MQTT-3.1.2-3]
						return session.sendInvalidProtocol()
					}
				case 2:
					session.keepAlive = uint16(rx[i]) << 8
				case 1:
					session.keepAlive |= uint16(rx[i])
					p.payload = p.payload[:0]
					session.rxState = payload
				default:
					return session.sendInvalidProtocol()
				}

				p.vhLen--
				p.remainingLength--
				i++
			case PUBLISH:
				if !p.gotVhLen {
					p.variableHeader = append(p.variableHeader, rx[i])
					p.vhLen++
					if p.vhLen == 2 {
						// vhLen is now remaining
						p.vhLen = uint32(p.variableHeader[0])<<8 | uint32(p.variableHeader[1])
						if p.flags&0x06 > 0 { // Qos > 0
							p.vhLen += 2
						}
						p.gotVhLen = true
					}

					p.remainingLength--
					i++
				} else {
					max := len(rx)
					if p.vhLen < uint32(max-i) {
						max = int(p.vhLen) + i
					}

					p.variableHeader = append(p.variableHeader, rx[i:max]...)
					got := uint32(max - i)
					p.vhLen -= got
					p.remainingLength -= got

					if p.vhLen == 0 {
						p.payload = p.payload[:0]
						session.rxState = payload
					}

					i = max
				}
			case SUBSCRIBE:
				avail := uint32(len(rx) - i)
				toRead := p.vhLen
				if avail < toRead {
					toRead = avail
				}

				p.variableHeader = append(p.variableHeader, rx[i:i+int(toRead)]...)
				p.vhLen -= toRead

				if p.vhLen == 0 {
					session.packet.identifier = binary.BigEndian.Uint16(p.variableHeader)
					p.payload = p.payload[:0]
					session.rxState = payload
				}

				p.remainingLength -= toRead
				i += int(toRead)
			default:
				// TODO: handle other variable headers
				p.remainingLength--
				i++
			}

			if p.remainingLength == 0 {
				session.rx <- struct{}{}
				session.rxState = controlAndFlags
			}

		case payload:
			max := len(rx)
			if p.remainingLength < uint32(max-i) {
				max = int(p.remainingLength) + i
			}

			p.payload = append(p.payload, rx[i:max]...)
			p.remainingLength -= uint32(max - i)

			if p.remainingLength == 0 {
				switch p.controlType {
				case CONNECT:
					s.handleConnect(session)
				case PUBLISH:
					s.handlePublish(session)
				case SUBSCRIBE:
					s.handleSubscribe(session)
				}
				session.rx <- struct{}{}
				session.rxState = controlAndFlags
			}

			i = max
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
	session.tx <- []byte{SUBACK << 4, 3, uint8(id >> 8), uint8(id), 0}
}

func (s *server) handlePublish(session *session) {
	p := &session.packet
	vh := p.variableHeader
	msg := p.payload
	topicLen := binary.BigEndian.Uint16(vh)
	if p.flags&0x06 > 0 { // Qos > 0
		p.identifier = uint16(vh[len(vh)-2]<<8) | uint16(vh[len(vh)-1])
	}

	fmt.Printf("Got publish from client '%s' to Topic '%s'. Msg: %s\n", session.clientId, vh[2:topicLen+2], string(msg))

	s.sLock.RLock() // cannot return early unless we make copy op details first
	s.cLock.RLock()
	for c := range s.subscriptions[string(vh[2:topicLen+2])] {
		client, ok := s.clients[c]
		if ok {
			pubPack := []byte{PUBLISH << 4}
			pubPack = append(pubPack, variableLengthEncode(int(topicLen)+len(msg)+2)...)
			pubPack = append(pubPack, vh[:topicLen+2]...) // 2 bytes + topic
			pubPack = append(pubPack, msg...)
			client.tx <-pubPack
		}
	}
	s.sLock.RUnlock()
	s.cLock.RUnlock()
}

func variableLengthEncode(l int) []byte {
	res := make([]byte, 0, 1)
	for {
		eb := l % 128
		l = l / 128
		if l > 0 {
			eb = eb | 128
		}
		res = append(res, byte(eb))
		if l<=0 {
			break
		}
	}
	return res
}

var unNamedClients int

func (s *server) handleConnect(session *session) {
	p := session.packet.payload
	if len(p) < 2 { // [MQTT-3.1.3-3]
		// TODO: confirm if this is correct behaviour, i.e. send anything before closing?
		session.close()
		log.Println("CONNECT packet payload error")
		return
	}

	clientIdLen := p[0]<<8 | p[1]
	if clientIdLen > 0 {
		session.clientId = string(p[2 : clientIdLen+2])
	} else {
		if session.stickySession() { // [MQTT-3.1.3-7]
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
	s.addClient(session)
	session.sendConnack(0)
	go session.startWriter()
}

func (s *server) addClient(newClient *session) {
	// [MQTT-3.1.2-4]
	log.Println("adding new client:", newClient.clientId)
	s.cLock.Lock()
	oldSession, present := s.clients[newClient.clientId]
	if present {
		log.Println("old session present. closing that connection")
		oldSession.close()
		if newClient.stickySession() && oldSession.stickySession() {
			newClient.notFirstSession = true
			log.Println("new client inheriting old session state")
			newClient.subscriptions = oldSession.subscriptions
		}
	}

	s.clients[newClient.clientId] = newClient
	s.cLock.Unlock()

	go newClient.watchDog()
}

func (s *server) removeClient(id string) {
	log.Println("deleting client session:", id)
	s.cLock.Lock()
	delete(s.clients, id)
	s.cLock.Unlock()
}

func (s *server) Start() error {
	l, err := net.Listen("tcp", ":1883")
	if err != nil {
		return err
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}

		go s.handleNewConn(conn)
	}
}

func (s *server) handleNewConn(conn net.Conn) {
	// TODO: Do not spin up session unless CONNECT received first, and in timely fashion.
	newSession := session{
		conn:          conn,
		tx:            make(chan []byte, 1),
		rx:            make(chan struct{}),
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
				if !newSession.stickySession() { // [MQTT-3.1.2-6]
					s.removeClient(newSession.clientId)
				}
				return
			}

			if strings.Contains(err.Error(), "use of closed") && newSession.serverClosedConn {
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
