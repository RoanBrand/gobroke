package broker

import (
	"crypto/tls"
	"errors"
	"io/ioutil"
	"net"
	"os"
	"strings"

	"github.com/RoanBrand/gobroke/config"
	"github.com/RoanBrand/gobroke/websocket"
	log "github.com/sirupsen/logrus"
)

const (
	serverBuffer = 1024
)

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
	}

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
	log.WithFields(log.Fields{
		"client": newses.clientId,
	}).Info("New session")

	// [MQTT-3.1.2-4]
	c, present := s.clients[newses.clientId]
	if present {
		log.WithFields(log.Fields{
			"client": newses.clientId,
		}).Debug("Old session present")

		newses.notFirstSession = true
		c.session.stop()

		if newses.persistent() && c.session.persistent() {
			log.WithFields(log.Fields{
				"client": newses.clientId,
			}).Debug("New session inheriting previous client state")
		} else {
			s.subscriptions.removeClient(c)
			c.clear <- struct{}{}
			<-c.clear
		}
		c.replaceSession(newses)
	} else {
		s.clients[newses.clientId] = newClient(newses)
		c = s.clients[newses.clientId]
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
	topics []string
	qoss   []uint8
}

type pub struct {
	topic  string
	pacs   [][]byte // packets of all QoS levels
	pubQoS uint8
	idLoc  int // index of publishID in packet when QoS > 0
	retain bool
	empty  bool // zero-byte payload
}

func makePub(topicUTF8, payload []byte, qos uint8) (p pub) {
	p.topic = string(topicUTF8[2:])
	p.pacs = make([][]byte, 3)
	p.pubQoS = qos
	tLen := len(topicUTF8)
	pLen := len(payload)
	if pLen == 0 {
		p.empty = true
	}

	// QoS 0
	p.pacs[0] = make([]byte, 1, 5+tLen+pLen) // ctrl 1 + remainLen 4max + topic(with2bytelen) + msgLen
	p.pacs[0][0] = PUBLISH
	p.pacs[0] = append(p.pacs[0], variableLengthEncode(tLen+pLen)...)
	p.pacs[0] = append(p.pacs[0], topicUTF8...) // 2 bytes + topic
	p.pacs[0] = append(p.pacs[0], payload...)

	// QoS 1
	if qos > 0 {
		p.pacs[1] = make([]byte, 1, 7+tLen+pLen) // ctrl 1 + remainLen 4max + topic(with2bytelen) + pID 2 + msgLen
		p.pacs[1][0] = PUBLISH | 0x02
		p.pacs[1] = append(p.pacs[1], variableLengthEncode(2+tLen+pLen)...)
		p.pacs[1] = append(p.pacs[1], topicUTF8...)
		p.idLoc = len(p.pacs[1])
		p.pacs[1] = append(p.pacs[1], 0, 0) // pID
		p.pacs[1] = append(p.pacs[1], payload...)

		// QoS 2
		if qos == 2 {
			// TODO: parse qos2 first before this works
		}
	}
	return
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
func (tt topicTree) removeClient(c *client) {
	var unSub func(topicTree, topT)
	unSub = func(sLevel topicTree, cLevel topT) {
		for l, nCl := range cLevel {
			if nSl, present := sLevel[l]; present {
				delete(nSl.subscribers, c)
				unSub(nSl.children, nCl.children)
			}
		}
	}

	unSub(tt, c.subscriptions)
}

// Add subscriptions for client. Also check for matching retained messages.
func (s *Server) addSubscriptions(subs *subList) {
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

	for i, t := range subs.topics {
		tLevels := strings.Split(t, "/")
		sLev, cLev := s.subscriptions, subs.c.subscriptions

		for n, tl := range tLevels {
			// Server subscriptions
			sT, present := sLev[tl]
			if !present {
				sLev[tl] = &topicLevel{}
				sT = sLev[tl]
				sT.init(size(n))
			}

			// Client's subscriptions
			sC, present := cLev[tl]
			if !present {
				cLev[tl] = &topL{}
				sC = cLev[tl]
				sC.children = make(topT, size(n))
			}

			if n < len(tLevels)-1 {
				sLev, cLev = sT.children, sC.children
			} else {
				sT.subscribers[subs.c] = subs.qoss[i]
			}
		}

		// Retained messages
		forwardLevel := func(l *retainLevel) {
			if l.p != nil {
				subs.c.pubRX <- subPub{l.p, subs.qoss[i], true}
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

func (s *Server) removeSubscriptions(subs *subList) {
	for _, t := range subs.topics {
		tLevels := strings.Split(t, "/")
		l := s.subscriptions

		for n, tl := range tLevels {
			nl, present := l[tl]
			if !present {
				break
			}

			if n < len(tLevels)-1 {
				l = nl.children
			} else {
				delete(nl.subscribers, subs.c)
			}
		}
	}
}

type subPub struct {
	p        *pub
	maxQoS   uint8
	retained bool
}

func (s *Server) forwardToSubscribers(tl *topicLevel, p *pub) {
	for c, maxQoS := range tl.subscribers {
		c.pubRX <- subPub{p, maxQoS, false}
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
	for n, tl := range tLevels {
		nl, ok := tr[tl]
		if !ok {
			tr[tl] = &retainLevel{children: make(retainTree, 1)}
			nl = tr[tl]
		}

		if n < len(tLevels)-1 {
			tr = nl.children
		} else {
			if p.empty {
				nl.p = nil
			} else {
				nl.p = p
			}
		}
	}
}

type retainLevel struct {
	p        *pub
	children retainTree
}

type retainTree map[string]*retainLevel
