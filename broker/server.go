package broker

import (
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"net"
	"os"
	"strings"

	"github.com/RoanBrand/gobroke/config"
	log "github.com/sirupsen/logrus"
)

const (
	serverBuffer = 1024
)

type Server struct {
	config        *config.Config
	clients       map[string]*client
	subscriptions topicTree

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
	newses.connectSent = true
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
	pacs   [][]byte
	pubQoS uint8
	idLoc  int
}

func (s *Server) publishToSubscribers(p *packet, from string) {
	topicLen := int(binary.BigEndian.Uint16(p.vh))
	topic := string(p.vh[2 : topicLen+2])
	qos := (p.flags & 0x06) >> 1

	lf := log.Fields{
		"client":  from,
		"topic":   topic,
		"QoS":     qos,
		"payload": string(p.payload),
	}
	if p.flags&0x08 > 0 {
		lf["duplicate"] = true
	}
	log.WithFields(lf).Debug("Got PUBLISH packet")

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
	if qos > 0 {
		pacs[1] = make([]byte, 1, 9+topicLen+pLen) // ctrl 1 + remainLen 4max + topicLen 2 + topicLen + pID 2 + msgLen
		pacs[1][0] = PUBLISH | 0x02
		pacs[1] = append(pacs[1], variableLengthEncode(topicLen+pLen+4)...)
		pacs[1] = append(pacs[1], p.vh[:topicLen+2]...) // 2 bytes + topic
		idLoc = len(pacs[1])
		pacs[1] = append(pacs[1], 0, 0) // pID
		pacs[1] = append(pacs[1], p.payload...)
	}

	s.pubs <- pub{topic: topic, pacs: pacs, pubQoS: qos, idLoc: idLoc}
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
			nSl, present := sLevel[l]
			if present {
				delete(nSl.subscribers, c)
				unSub(nSl.children, nCl.children)
			}
		}
	}

	unSub(tt, c.subscriptions)
}

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
			sT, present := sLev[tl]
			if !present {
				sLev[tl] = &topicLevel{}
				sT = sLev[tl]
				sT.init(size(n))
			}

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
	p      *pub
	maxQoS uint8
}

func (s *Server) forwardToSubscribers(tl *topicLevel, p *pub) {
	for c, maxQoS := range tl.subscribers {
		c.pubRX <- subPub{p, maxQoS}
	}
}

func (s *Server) matchSubscriptions(p *pub) {
	tLevels := strings.Split(p.topic, "/")
	var matchLevel func(topicTree, int)

	matchLevel = func(l topicTree, n int) {
		// direct match
		if nl, ok := l[tLevels[n]]; ok {
			if n == len(tLevels)-1 {
				s.forwardToSubscribers(nl, p)
				if nl, ok := nl.children["#"]; ok { // # match - next level
					s.forwardToSubscribers(nl, p)
				}
			} else {
				matchLevel(nl.children, n+1)
			}
		}

		// # match
		if nl, ok := l["#"]; ok {
			s.forwardToSubscribers(nl, p)
		}

		// + match
		if nl, ok := l["+"]; ok {
			if n == len(tLevels)-1 {
				s.forwardToSubscribers(nl, p)
				if nl, ok := nl.children["#"]; ok { // # match - next level
					s.forwardToSubscribers(nl, p)
				}
			} else {
				matchLevel(nl.children, n+1)
			}
		}
	}
	matchLevel(s.subscriptions, 0)
}
