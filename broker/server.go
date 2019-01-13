package broker

import (
	"net"
	"sync"

	log "github.com/sirupsen/logrus"
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
