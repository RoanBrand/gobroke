package broker

import (
	"log"
	"net"
	"sync"
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
