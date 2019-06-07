package broker

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strings"

	"github.com/RoanBrand/gobroke/config"
	"github.com/RoanBrand/gobroke/internal/store"
	"github.com/RoanBrand/gobroke/websocket"
	log "github.com/sirupsen/logrus"
)

const (
	serverBuffer = 1024
)

type serverStore interface {
	LoadSubs(iter func(topic, cID string, subQoS uint8)) error
	AddSubs(cID []byte, topics [][]byte, QoSs []uint8) error
	RemoveSubs(cID []byte, topics [][]byte) error

	LoadPubs(cID []byte, iter func(p []byte, QoS uint8, pubID uint16)) error
	ForwardPub(cID, packet []byte, QoS uint8, pubIDIdx uint32) (pubID uint16, err error)
	RemovePub(cID []byte, QoS uint8, pubID uint16) error

	Close() error
}

type Server struct {
	config        *config.Config
	clients       map[string]*client
	subscriptions topicTree
	retained      retainTree

	errs        chan error
	register    chan *session
	unregister  chan *session
	subscribe   chan subList
	unsubscribe chan subList
	pubs        chan pub

	tcpL, tlsL net.Listener

	state serverStore
}

func NewServer(confPath string) (*Server, error) {
	conf, err := config.New(confPath)
	if err != nil {
		return nil, err
	}

	if conf.Log.File != "" {
		f, err := os.OpenFile(conf.Log.File, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
		log.SetOutput(f)
	}
	if conf.Log.Level != "" {
		switch strings.ToLower(conf.Log.Level) {
		case "error":
			log.SetLevel(log.ErrorLevel)
		case "warn":
			log.SetLevel(log.WarnLevel)
		case "info":
			log.SetLevel(log.InfoLevel)
		case "debug":
			log.SetLevel(log.DebugLevel)
		default:
			return nil, errors.New("unknown log level: " + conf.Log.Level)
		}
	}

	sstore, err := store.NewDiskStore(`c:\mqttdatadir`)
	if err != nil {
		return nil, err
	}

	s := Server{
		config:        conf,
		clients:       make(map[string]*client, 16),
		subscriptions: make(topicTree, 4),
		retained:      make(retainTree, 4),
		errs:          make(chan error),
		register:      make(chan *session),
		unregister:    make(chan *session),
		subscribe:     make(chan subList),
		unsubscribe:   make(chan subList),
		pubs:          make(chan pub, serverBuffer),

		state: sstore,
	}

	// Saved state
	err = s.loadSubscriptions()
	if err != nil {
		return nil, err
	}

	// Network connections.
	if err := s.setupTCP(); err != nil {
		return nil, err
	}
	if err := s.setupTLS(); err != nil {
		return nil, err
	}

	websocket.SetDispatcher(s.startSession)
	if err := s.setupWebsocket(); err != nil {
		return nil, err
	}
	if err := s.setupWebsocketSecure(); err != nil {
		return nil, err
	}

	return &s, nil
}

func (s *Server) Start() error {
	go s.run()
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
	s.state.Close()
}

func (s *Server) run() {
	lf := make(log.Fields, 4)
	if s.config.TCP.Enabled {
		lf["tcp_address"] = s.config.TCP.Address
	}
	if s.config.TLS.Enabled {
		lf["tls_address"] = s.config.TLS.Address
	}
	if s.config.WS.Enabled {
		lf["ws_address"] = s.config.WS.Address
	}
	if s.config.WSS.Enabled {
		lf["wss_address"] = s.config.WSS.Address
	}
	log.WithFields(lf).Info("Starting MQTT server")

	for {
		select {
		case ses := <-s.register:
			s.addSession(ses)
		case ses := <-s.unregister:
			s.removeClient(ses.client)
		case sl := <-s.subscribe:
			s.addSubscriptions(&sl)
		case sl := <-s.unsubscribe:
			s.removeSubscriptions(&sl)
		case p := <-s.pubs:
			s.matchSubscriptions(&p)
		}
	}
}

func (s *Server) setupTCP() error {
	if !s.config.TCP.Enabled {
		return nil
	}

	l, err := net.Listen("tcp", s.config.TCP.Address)
	if err != nil {
		return err
	}

	s.tcpL = l
	go s.startDispatcher(l)
	return nil
}

func (s *Server) setupTLS() error {
	if !s.config.TLS.Enabled {
		return nil
	}

	cf, err := os.Open(s.config.TLS.Cert)
	if err != nil {
		return err
	}
	defer cf.Close()
	kf, err := os.Open(s.config.TLS.Key)
	if err != nil {
		return err
	}
	defer kf.Close()

	cert, err := ioutil.ReadAll(cf)
	if err != nil {
		return err
	}
	key, err := ioutil.ReadAll(kf)
	if err != nil {
		return err
	}

	kp, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return err
	}
	config := tls.Config{Certificates: []tls.Certificate{kp}}

	l, err := tls.Listen("tcp", s.config.TLS.Address, &config)
	if err != nil {
		return err
	}

	s.tlsL = l
	go s.startDispatcher(l)
	return nil
}

func (s *Server) setupWebsocket() error {
	if !s.config.WS.Enabled {
		return nil
	}

	return websocket.Setup(s.config.WS.Address, s.config.WS.CheckOrigin, s.errs)
}

func (s *Server) setupWebsocketSecure() error {
	c := &s.config.WSS
	if !c.Enabled {
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

func (s *Server) addSession(newses *session) {
	cID := newses.clientId
	log.WithFields(log.Fields{
		"client": cID,
	}).Info("New session")

	// [MQTT-3.1.2-4]
	c, present := s.clients[cID]
	if present {
		log.WithFields(log.Fields{
			"client": cID,
		}).Debug("Old session present")

		newses.notFirstSession = true
		c.session.stop()

		if newses.persistent() && c.session.persistent() {
			log.WithFields(log.Fields{
				"client": cID,
			}).Debug("New session inheriting previous client state")
		} else {
			s.subscriptions.removeClient(c)
			c.clear <- struct{}{}
			<-c.clear
		}
		c.replaceSession(newses)
	} else {
		s.clients[cID] = newClient(newses)
		c = s.clients[cID]

		// load any queued messages from disk
		cIDLen := uint16(len(cID))
		err := s.state.LoadPubs(append([]byte{byte(cIDLen >> 8), byte(cIDLen)}, []byte(cID)...), func(p []byte, QoS uint8, pubID uint16) {
			switch QoS {
			case 0:
				c.q0.Add(p)
			case 1:
				c.q1.Add(pubID, p)
			case 2:
				c.q2.Add(pubID, p)
			}
		})
		if err != nil {
			log.Error(err)
		}
	}

	newses.client = c
	newses.sendConnack(0)
	newses.run()
}

func (s *Server) removeClient(c *client) {
	if c == nil {
		return
	}

	log.WithFields(log.Fields{
		"client": c.session.clientId,
	}).Debug("Deleting client session (CleanSession)")

	s.subscriptions.removeClient(c)
	delete(s.clients, c.session.clientId)
}

type subList struct {
	c      *client
	topics [][]byte // MQTT UTF-8. 2 byte len + str
	qoss   []uint8
}

type pub struct {
	topic  string
	pacs   [][]byte // packets of all QoS levels
	pubQoS uint8
	idLoc  uint32 // index of publishID in packet when QoS > 0
	retain bool   // this pub is to be retained on the topic
	empty  bool   // zero-byte payload
}

func makePub(topicUTF8, payload []byte, qos uint8, retain bool) (p pub) {
	p.topic = string(topicUTF8[2:])
	if qos == 0 {
		p.pacs = make([][]byte, 1)
	} else {
		p.pacs = make([][]byte, 2)
	}
	p.pubQoS = qos
	p.retain = retain
	tLen := len(topicUTF8)
	pLen := len(payload)
	p.empty = pLen == 0

	// QoS 0
	p.pacs[0] = make([]byte, 1, 5+tLen+pLen) // ctrl 1 + remainLen 4max + topic(with2bytelen) + msgLen
	p.pacs[0][0] = PUBLISH
	p.pacs[0] = variableLengthEncode(p.pacs[0], tLen+pLen)
	p.pacs[0] = append(p.pacs[0], topicUTF8...) // 2 bytes + topic
	p.pacs[0] = append(p.pacs[0], payload...)

	// QoS 1 & 2 - each client needs to mask control byte with QoS, and set pubID
	if qos > 0 {
		p.pacs[1] = make([]byte, 1, 7+tLen+pLen) // ctrl 1 + remainLen 4max + topic(with2bytelen) + pID 2 + msgLen
		p.pacs[1][0] = PUBLISH                   // unmasked
		p.pacs[1] = variableLengthEncode(p.pacs[1], 2+tLen+pLen)
		p.pacs[1] = append(p.pacs[1], topicUTF8...)
		p.idLoc = uint32(len(p.pacs[1]))
		p.pacs[1] = append(p.pacs[1], 0, 0) // pID unset
		p.pacs[1] = append(p.pacs[1], payload...)
	}
	return
}

type topicLevel struct {
	children    topicTree
	subscribers map[string]uint8 // clientID -> QoS level
}

func (tl *topicLevel) init(size int) {
	tl.children = make(topicTree, size)
	tl.subscribers = make(map[string]uint8, size)
}

type topicTree map[string]*topicLevel // level -> sub levels

// Remove all subscriptions of client.
func (tt topicTree) removeClient(c *client) {
	var unSub func(topicTree, topT)
	unSub = func(sLevel topicTree, cLevel topT) {
		for l, nCl := range cLevel {
			if nSl, present := sLevel[l]; present {
				delete(nSl.subscribers, c.session.clientId)
				unSub(nSl.children, nCl.children)
			}
		}
	}

	unSub(tt, c.subscriptions)
}

// Load all saved client subscriptions on startup.
func (s *Server) loadSubscriptions() error {
	first := true
	var oldT string
	var sT *topicLevel

	return s.state.LoadSubs(func(topic, cID string, subQoS uint8) {
		if first || topic != oldT {
			tLevels := strings.Split(topic, "/")
			sLev := s.subscriptions

			var ok bool
			for n, tl := range tLevels {
				if sT, ok = sLev[tl]; !ok {
					sLev[tl] = &topicLevel{}
					sT = sLev[tl]
					sT.init(size(n))
				}

				sLev = sT.children
			}
		}

		fmt.Println("add server sub for clientID", cID, "Topic:", topic)
		sT.subscribers[cID] = subQoS
		first, oldT = false, topic
	})
}

// Add subscriptions for client. Also check for matching retained messages.
func (s *Server) addSubscriptions(subs *subList) {
	cID := subs.c.session.clientId
	cIDLen := uint16(len(cID))
	err := s.state.AddSubs(append([]byte{byte(cIDLen >> 8), byte(cIDLen)}, []byte(cID)...), subs.topics, subs.qoss)
	if err != nil {
		log.Fatal(err)
	}

	for i, t := range subs.topics {
		tLevels := strings.Split(string(t[2:]), "/")
		sLev, cLev := s.subscriptions, subs.c.subscriptions

		var sT *topicLevel
		var ok bool
		for n, tl := range tLevels {
			// Server subscriptions
			if sT, ok = sLev[tl]; !ok {
				sLev[tl] = &topicLevel{}
				sT = sLev[tl]
				sT.init(size(n))
			}

			// Client's subscriptions
			sC, ok := cLev[tl]
			if !ok {
				cLev[tl] = &topL{}
				sC = cLev[tl]
				sC.children = make(topT, size(n))
			}

			sLev, cLev = sT.children, sC.children
		}
		sT.subscribers[cID] = subs.qoss[i]

		// Retained messages
		forwardLevel := func(l *retainLevel) {
			if l.p != nil {
				s.fwdPubToSub(cID, l.p, subs.qoss[i], true)
				// subs.c.pubRX <- subPub{l.p, subs.qoss[i], true}
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
			switch tLevels[n] {
			case "#":
				forwardAll(l)
			case "+":
				switch len(tLevels) - n {
				case 1:
					for _, nl := range l {
						forwardLevel(nl)
					}
				case 2:
					if tLevels[len(tLevels)-1] == "#" {
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
				nl, ok := l[tLevels[n]]
				if !ok {
					break
				}

				switch len(tLevels) - n {
				case 1:
					forwardLevel(nl)
				case 2:
					if tLevels[len(tLevels)-1] == "#" {
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

// TODO: remove subs from client state as well.
func (s *Server) removeSubscriptions(subs *subList) {
	cID := subs.c.session.clientId
	cIDLen := uint16(len(cID))
	err := s.state.RemoveSubs(append([]byte{byte(cIDLen >> 8), byte(cIDLen)}, []byte(cID)...), subs.topics)
	if err != nil {
		log.Fatal(err)
	}

loop:
	for _, t := range subs.topics {
		tLevels := strings.Split(string(t[2:]), "/")
		l := s.subscriptions

		var nl *topicLevel
		var ok bool
		for _, tl := range tLevels {
			if nl, ok = l[tl]; !ok {
				continue loop
			}
			l = nl.children
		}
		delete(nl.subscribers, subs.c.session.clientId)
	}
}

/*type subPub struct {
	p        *pub
	maxQoS   uint8
	retained bool // true: this is pub is being sent out as result from client subscription
}*/

// Forward PUBLISH message to subscribed client, providing pub msg, subscription QoS level & if pub retained due to sub.
func (s *Server) fwdPubToSub(cID string, p *pub, maxQoS uint8, retained bool) {
	finalQoS := p.pubQoS
	if maxQoS < p.pubQoS {
		finalQoS = maxQoS
	}

	cIDLen := uint16(len(cID))
	if finalQoS == 0 {
		var pubP []byte
		if c, ok := s.clients[cID]; ok {
			// TODO: add to queue, but will get lost if session not active, and service stops before client reconnect
			if retained {
				// Make copy, otherwise we race with original packet being sent out without retain flag set.
				pubP = make([]byte, len(p.pacs[0]))
				copy(pubP, p.pacs[0])
				pubP[0] |= 0x01
			} else {
				pubP = p.pacs[0]
			}

			c.q0.Add(pubP)
		} else {
			_, err := s.state.ForwardPub(append([]byte{byte(cIDLen >> 8), byte(cIDLen)}, []byte(cID)...), p.pacs[0], 0, 0)
			if err != nil {
				log.Error(err)
				return
			}
		}

	} else { // QoS 1 & 2
		p.pacs[1][0] &= 0xF9
		p.pacs[1][0] |= 1 << finalQoS
		if retained {
			p.pacs[1][0] |= 0x01
		}

		pubIDUsed, err := s.state.ForwardPub(append([]byte{byte(cIDLen >> 8), byte(cIDLen)}, []byte(cID)...), p.pacs[1], finalQoS, p.idLoc)
		if err != nil {
			log.Error(err)
			return
		}

		if c, ok := s.clients[cID]; ok {
			pubP := make([]byte, len(p.pacs[1]))
			copy(pubP, p.pacs[1])

			if finalQoS == 1 {
				c.q1.Add(pubIDUsed, pubP)
			} else {
				c.q2.Add(pubIDUsed, pubP)
			}
		}
	}
}

func (s *Server) forwardToSubscribers(tl *topicLevel, p *pub) {
	for cID, maxQoS := range tl.subscribers {
		s.fwdPubToSub(cID, p, maxQoS, false)
	}
}

// Match published message topic to all subscribers, and forward.
// Also store pub if retained message.
func (s *Server) matchSubscriptions(p *pub) {
	tLevels := strings.Split(p.topic, "/")
	var matchLevel func(topicTree, int)

	matchLevel = func(l topicTree, n int) {
		// direct match
		if nl, ok := l[tLevels[n]]; ok {
			if n < len(tLevels)-1 {
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
			if n < len(tLevels)-1 {
				matchLevel(nl.children, n+1)
			} else {
				s.forwardToSubscribers(nl, p)
				if nl, ok := nl.children["#"]; ok { // # match - next level
					s.forwardToSubscribers(nl, p)
				}
			}
		}
	}
	matchLevel(s.subscriptions, 0)

	if !p.retain {
		return
	}

	tr := s.retained
	var nl *retainLevel
	var ok bool
	for _, tl := range tLevels {
		if nl, ok = tr[tl]; !ok {
			tr[tl] = &retainLevel{children: make(retainTree, 1)}
			nl = tr[tl]
		}

		tr = nl.children
	}
	if p.empty {
		nl.p = nil // delete existing retained message
	} else {
		nl.p = p
	}
}

type retainLevel struct {
	p        *pub
	children retainTree
}

type retainTree map[string]*retainLevel

func size(n int) (s int) {
	if n < 8 {
		s = 4
	} else if n < 16 {
		s = 2
	} else {
		s = 1
	}
	return
}
