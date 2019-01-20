package broker

import (
	"encoding/binary"
	"net"
	"sync"

	log "github.com/sirupsen/logrus"
)

type server struct {
	clients map[string]*session
	cLock   sync.RWMutex

	subscriptions map[string]map[string]uint8 // topic -> subscribed client id -> QoS level
	sLock         sync.RWMutex
}

func NewServer() *server {
	return &server{
		clients:       make(map[string]*session, 16),
		subscriptions: make(map[string]map[string]uint8, 4),
	}
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

func (s *server) addClient(newClient *session) {
	log.WithFields(log.Fields{
		"id": newClient.clientId,
	}).Info("New client")

	// [MQTT-3.1.2-4]
	s.cLock.Lock()
	oldSession, present := s.clients[newClient.clientId]
	if present {
		log.WithFields(log.Fields{
			"id": newClient.clientId,
		}).Debug("Old session present. Closing that connection")
		oldSession.close()

		if newClient.persistent() && oldSession.persistent() {
			newClient.notFirstSession = true
			log.WithFields(log.Fields{
				"id": newClient.clientId,
			}).Debug("New client inheriting old session state")
			newClient.subscriptions = oldSession.subscriptions
		}
	}

	s.clients[newClient.clientId] = newClient
	s.cLock.Unlock()
}

func (s *server) removeClient(id string) {
	log.WithFields(log.Fields{
		"id": id,
	}).Debug("Deleting client session (CleanSession)")
	s.cLock.Lock()
	delete(s.clients, id)
	s.cLock.Unlock()
}

func (s *server) addSubscriptions(topics []string, qoss []uint8, clientId string) {
	s.sLock.Lock()
	for i, t := range topics {
		cl, ok := s.subscriptions[t]
		if !ok {
			s.subscriptions[t] = make(map[string]uint8, 8)
			cl = s.subscriptions[t]
		}

		cl[clientId] = qoss[i]
	}
	s.sLock.Unlock()
}

func (s *server) publishToSubscribers(p *packet, from string) {
	topicLen := int(binary.BigEndian.Uint16(p.vh))
	topic := string(p.vh[2 : topicLen+2])

	log.WithFields(log.Fields{
		"id":        from,
		"Topic":     topic,
		"QoS":       p.flags & 0x06 >> 1,
		"Duplicate": p.flags&0x08 > 0,
		"Payload":   string(p.payload),
	}).Debug("Got PUBLISH packet")

	pacs := make([][]byte, 3)
	pLen := len(p.payload)

	// QoS 0
	pacs[0] = make([]byte, 1, 7+topicLen+pLen) // ctrl 1 + remainLen 4max + topicLen 2 + topicLen + msgLen
	pacs[0][0] = PUBLISH
	pacs[0] = append(pacs[0], variableLengthEncode(topicLen+pLen+2)...)
	pacs[0] = append(pacs[0], p.vh[:topicLen+2]...) // 2 bytes + topic
	pacs[0] = append(pacs[0], p.payload...)

	// QoS 1
	var idLoc int
	if p.flags&0x06 > 0 {
		pacs[1] = make([]byte, 1, 9+topicLen+pLen) // ctrl 1 + remainLen 4max + topicLen 2 + topicLen + pID 2 + msgLen
		pacs[1][0] = PUBLISH | 0x02
		pacs[1] = append(pacs[1], variableLengthEncode(topicLen+pLen+4)...)
		pacs[1] = append(pacs[1], p.vh[:topicLen+2]...) // 2 bytes + topic
		idLoc = len(pacs[1])
		pacs[1] = append(pacs[1], 0, 0) // pID
		pacs[1] = append(pacs[1], p.payload...)
	}

	go s.forwardToSubscribers(topic, pacs, p.flags&0x06>>1, idLoc)
}

func (s *server) forwardToSubscribers(topic string, pacs [][]byte, pubQoS uint8, idLoc int) {
	s.sLock.RLock()
	s.cLock.RLock()

	for cId, maxQoS := range s.subscriptions[topic] {
		client, ok := s.clients[cId]
		if !ok {
			continue
		}

		finalQoS := pubQoS
		if maxQoS < pubQoS {
			finalQoS = maxQoS
		}
		switch finalQoS {
		case 0:
			client.writePacket(pacs[0])
		case 1:
			pubP := make([]byte, len(pacs[1]))
			copy(pubP, pacs[1])
			client.publishId++
			pubP[idLoc] = uint8(client.publishId >> 8)
			pubP[idLoc+1] = uint8(client.publishId)

			go client.sendQoS1Message(pubP, client.publishId)
		}
	}

	s.sLock.RUnlock()
	s.cLock.RUnlock()
}
