package gobroke

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/RoanBrand/gobroke/internal/config"
	"github.com/RoanBrand/gobroke/internal/model"
	"github.com/RoanBrand/gobroke/internal/queue"
	"github.com/RoanBrand/gobroke/internal/websocket"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	config.Config
	errs       chan error
	tcpL, tlsL net.Listener

	sesLock sync.Mutex
	clients map[string]*client

	subLock       sync.RWMutex
	subscriptions topicTree
	retained      retainTree

	pubs queue.Basic
}

func (s *Server) Run() error {
	if s.TCP.Address == "" && s.TLS.Address == "" && s.WS.Address == "" && s.WSS.Address == "" {
		s.TCP.Address = ":1883" // default to basic TCP only server if nothing specified.
	}

	s.errs = make(chan error)
	s.clients = make(map[string]*client, 16)
	s.subscriptions = make(topicTree, 4)
	s.retained = make(retainTree, 4)

	s.setupLogging()
	s.pubs.Init()

	if err := s.setupTCP(); err != nil {
		return err
	}
	if err := s.setupTLS(); err != nil {
		return err
	}

	websocket.SetDispatcher(s.startSession)
	if err := s.setupWebsocket(); err != nil {
		return err
	}
	if err := s.setupWebsocketSecure(); err != nil {
		return err
	}

	lf := make(log.Fields, 4)
	if s.TCP.Address != "" {
		lf["tcp_address"] = s.TCP.Address
	}
	if s.TLS.Address != "" {
		lf["tls_address"] = s.TLS.Address
	}
	if s.WS.Address != "" {
		lf["ws_address"] = s.WS.Address
	}
	if s.WSS.Address != "" {
		lf["wss_address"] = s.WSS.Address
	}
	log.WithFields(lf).Info("Starting MQTT server")

	go s.pubs.StartDispatcher(func(i *queue.Item) error {
		s.matchSubscriptions(i.P)
		return nil
	}, nil, nil)

	return <-s.errs
}

func (s *Server) Stop() {
	log.Info("Shutting down MQTT server")
	if s.tcpL != nil {
		s.tcpL.Close()
	}
	if s.tlsL != nil {
		s.tlsL.Close()
	}
}

func (s *Server) setupLogging() error {
	if s.Log.File != "" {
		f, err := os.OpenFile(s.Log.File, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		log.SetOutput(f)
	}
	if s.Log.Level != "" {
		switch strings.ToLower(s.Log.Level) {
		case "error":
			log.SetLevel(log.ErrorLevel)
		case "warn":
			log.SetLevel(log.WarnLevel)
		case "info":
			log.SetLevel(log.InfoLevel)
		case "debug":
			log.SetLevel(log.DebugLevel)
		default:
			return errors.New("unknown log level: " + s.Log.Level)
		}
	}

	return nil
}

func (s *Server) setupTCP() error {
	if s.TCP.Address == "" {
		return nil
	}

	l, err := net.Listen("tcp", s.TCP.Address)
	if err != nil {
		return err
	}

	s.tcpL = l
	go s.startDispatcher(l)
	return nil
}

func (s *Server) setupTLS() error {
	if s.TLS.Address == "" {
		return nil
	}

	cert, err := os.ReadFile(s.TLS.Cert)
	if err != nil {
		return err
	}

	key, err := os.ReadFile(s.TLS.Key)
	if err != nil {
		return err
	}

	kp, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return err
	}
	config := tls.Config{Certificates: []tls.Certificate{kp}}

	l, err := tls.Listen("tcp", s.TLS.Address, &config)
	if err != nil {
		return err
	}

	s.tlsL = l
	go s.startDispatcher(l)
	return nil
}

func (s *Server) setupWebsocket() error {
	if s.WS.Address == "" {
		return nil
	}

	return websocket.Setup(s.WS.Address, s.WS.CheckOrigin, s.errs)
}

func (s *Server) setupWebsocketSecure() error {
	c := &s.WSS
	if c.Address == "" {
		return nil
	}

	return websocket.SetupTLS(c.Address, c.Cert, c.Key, c.CheckOrigin, s.errs)
}

func (s *Server) startDispatcher(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			if strings.Contains(err.Error(), "use of closed") {
				err = nil
			}
			s.errs <- err
			return
		}

		go s.startSession(conn)
	}
}

// returns true if session is present and if state is reused.
func (s *Server) addSession(ses *session) bool {
	log.WithFields(log.Fields{
		"client": ses.clientId,
	}).Info("New session")

	sendSP := false

	s.sesLock.Lock()
	c, ok := s.clients[ses.clientId] // [MQTT-3.1.2-4]
	if ok {
		c.session.stop()
		log.WithFields(log.Fields{
			"client": ses.clientId,
		}).Debug("Old session present")

		if ses.persistent() && c.session.persistent() {
			sendSP = true
		} else {
			s.removeClientSubscriptions(c) // [MQTT-3.1.2-6]
			c.clearState()
		}
		c.replaceSession(ses)
	} else {
		s.clients[ses.clientId] = newClient(ses)
		c = s.clients[ses.clientId]
	}
	ses.client = c
	s.sesLock.Unlock()

	if sendSP {
		log.WithFields(log.Fields{
			"client": ses.clientId,
		}).Debug("New session inheriting previous client state")
	}

	return sendSP
}

func (s *Server) removeSession(ses *session) {
	s.sesLock.Lock()
	defer s.sesLock.Unlock()
	// check if another new session has not taken over already
	c, ok := s.clients[ses.clientId]
	if !ok || c.session != ses {
		return
	}

	log.WithFields(log.Fields{
		"client": ses.clientId,
	}).Debug("Deleting client session (CleanSession)")

	s.removeClientSubscriptions(ses.client)
	delete(s.clients, ses.clientId)
}

type topicLevel struct {
	children    topicTree
	subscribers map[*client]uint8 // client -> QoS level
}

func (tl *topicLevel) init(size int) {
	tl.children = make(topicTree, size)
	tl.subscribers = make(map[*client]uint8, size)
}

type topicTree map[string]*topicLevel // level -> sub levels

// Remove all subscriptions of client.
func (s *Server) removeClientSubscriptions(c *client) {
	var unSub func(topicTree, topT)
	unSub = func(sLevel topicTree, cLevel topT) {
		for l, cTL := range cLevel {
			sTL := sLevel[l]

			if cTL.subscribed {
				cTL.subscribed = false
				delete(sTL.subscribers, c)
			}
			unSub(sTL.children, cTL.children)
		}
	}

	s.subLock.Lock()
	unSub(s.subscriptions, c.subscriptions)
	s.subLock.Unlock()
}

// Add subscriptions for client. Also check for matching retained messages.
func (s *Server) addSubscriptions(c *client, topics [][]string, qoss []uint8) {
	size := func(n int) (s int) {
		if n < 8 {
			s = 4
		} else if n < 16 {
			s = 2
		} else {
			s = 1
		}
		return
	}

	s.subLock.Lock()
	defer s.subLock.Unlock()

	for i, t := range topics {
		sLev, cLev := s.subscriptions, c.subscriptions

		var sTL *topicLevel
		var cTL *topL
		var ok bool
		for n, tl := range t {
			// Server subscriptions
			if sTL, ok = sLev[tl]; !ok {
				sLev[tl] = &topicLevel{}
				sTL = sLev[tl]
				sTL.init(size(n))
			}

			// Client's subscriptions
			if cTL, ok = cLev[tl]; !ok {
				cLev[tl] = &topL{}
				cTL = cLev[tl]
				cTL.children = make(topT, size(n))
			}

			sLev, cLev = sTL.children, cTL.children
		}
		sTL.subscribers[c] = qoss[i]
		cTL.subscribed = true

		// Retained messages
		forwardLevel := func(l *retainLevel) {
			if l.p != nil {
				c.processPub(l.p, qoss[i], true)
			}
		}

		var forwardAll func(retainTree)
		forwardAll = func(l retainTree) {
			for _, nl := range l {
				forwardLevel(nl)
				forwardAll(nl.children)
			}
		}

		var matchLevel func(retainTree, int)
		matchLevel = func(l retainTree, n int) {
			switch t[n] {
			case "#":
				forwardAll(l)
			case "+":
				switch len(t) - n {
				case 1:
					for _, nl := range l {
						forwardLevel(nl)
					}
				case 2:
					if t[len(t)-1] == "#" {
						for _, nl := range l {
							forwardLevel(nl)
						}
					}
					fallthrough
				default:
					for _, nl := range l {
						matchLevel(nl.children, n+1)
					}
				}
			default: // direct match
				nl, ok := l[t[n]]
				if !ok {
					break
				}

				switch len(t) - n {
				case 1:
					forwardLevel(nl)
				case 2:
					if t[len(t)-1] == "#" {
						forwardLevel(nl)
					}
					fallthrough
				default:
					matchLevel(nl.children, n+1)
				}
			}
		}

		matchLevel(s.retained, 0)
	}
}

func (s *Server) removeSubscriptions(c *client, topics [][]string) {
	var sTL *topicLevel
	var cTL *topL
	var ok bool

	s.subLock.Lock()
	defer s.subLock.Unlock()

loop:
	for _, t := range topics {
		sl, cl := s.subscriptions, c.subscriptions

		for _, tl := range t {
			// Server
			if sTL, ok = sl[tl]; !ok {
				continue loop // no one subscribed to this
			}

			// Client
			if cTL, ok = cl[tl]; !ok {
				continue loop // client not subscribed
			}

			sl, cl = sTL.children, cTL.children
		}

		delete(sTL.subscribers, c)
		cTL.subscribed = false
	}
}

func (s *Server) forwardToSubscribers(tl *topicLevel, p model.PubMessage) {
	for c, maxQoS := range tl.subscribers {
		c.processPub(p, maxQoS, false)
	}
}

// Match published message topic to all subscribers, and forward.
// Also store pub if retained message.
func (s *Server) matchSubscriptions(p model.PubMessage) {
	tLen := int(binary.BigEndian.Uint16(p[1:]))
	topic := strings.Split(string(p[3:3+tLen]), "/")

	var matchLevel func(topicTree, int)
	matchLevel = func(l topicTree, n int) {
		// direct match
		if nl, ok := l[topic[n]]; ok {
			if n < len(topic)-1 {
				matchLevel(nl.children, n+1)
			} else {
				s.forwardToSubscribers(nl, p)
				if nl, ok := nl.children["#"]; ok { // # match - next level
					s.forwardToSubscribers(nl, p)
				}
			}
		}

		// # match
		if nl, ok := l["#"]; ok {
			s.forwardToSubscribers(nl, p)
		}

		// + match
		if nl, ok := l["+"]; ok {
			if n < len(topic)-1 {
				matchLevel(nl.children, n+1)
			} else {
				s.forwardToSubscribers(nl, p)
				if nl, ok := nl.children["#"]; ok { // # match - next level
					s.forwardToSubscribers(nl, p)
				}
			}
		}
	}

	s.subLock.RLock()
	defer s.subLock.RUnlock()

	matchLevel(s.subscriptions, 0)

	if !p.Retain() {
		return
	}

	tr := s.retained
	var nl *retainLevel
	var ok bool
	for _, tl := range topic {
		if nl, ok = tr[tl]; !ok {
			tr[tl] = &retainLevel{children: make(retainTree, 1)}
			nl = tr[tl]
		}

		tr = nl.children
	}

	if len(p) == 3+tLen {
		// payload empty, so delete existing retained message
		nl.p = nil
	} else {
		nl.p = p
	}
}

type retainLevel struct {
	p        model.PubMessage
	children retainTree
}

type retainTree map[string]*retainLevel
