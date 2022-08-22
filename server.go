package gobroke

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/RoanBrand/gobroke/internal/config"
	"github.com/RoanBrand/gobroke/internal/model"
	"github.com/RoanBrand/gobroke/internal/queue"
	"github.com/RoanBrand/gobroke/internal/websocket"
	log "github.com/sirupsen/logrus"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Server struct {
	config.Config
	Auther

	ctx    context.Context
	cancel context.CancelFunc

	tcpL, tlsL net.Listener
	ws, wss    websocket.WS

	sesLock sync.Mutex
	clients map[string]*client

	subLock       sync.RWMutex
	subscriptions topicTree
	retained      retainTree

	pubs queue.Basic

	sepPubTopic [][]byte
}

func (s *Server) Run() error {
	if s.TCP.Address == "" && s.TLS.Address == "" && s.WS.Address == "" && s.WSS.Address == "" {
		s.TCP.Address = ":1883" // default to basic TCP only server if nothing specified.
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	defer s.cancel()

	s.clients = make(map[string]*client)
	s.subscriptions = make(topicTree)
	s.retained = make(retainTree)

	s.setupLogging()
	s.pubs.Init()

	if err := s.setupTCP(); err != nil {
		return err
	}
	if err := s.setupTLS(); err != nil {
		return err
	}

	s.ws.Run(s.ctx, s.WS.Address, s.WS.CheckOrigin, s.startSession)
	s.wss.RunTLS(s.ctx, s.WSS.Address, s.WSS.Cert, s.WSS.Key, s.WSS.CheckOrigin, s.startSession)

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

	go s.pubs.StartDispatcher(s.ctx, s.matchSubscriptions, nil)

	<-s.ctx.Done()
	err := s.ctx.Err()
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	return err
}

func (s *Server) Close() error {
	s.Shutdown()
	return nil
}

func (s *Server) Shutdown() {
	log.Info("Shutting down MQTT server")

	if s.tcpL != nil {
		if err := s.tcpL.Close(); err != nil {
			log.Errorln(err)
		}
	}
	if s.tlsL != nil {
		if err := s.tlsL.Close(); err != nil {
			log.Errorln(err)
		}
	}
	if err := s.ws.Stop(); err != nil {
		log.Errorln(err)
	}
	if err := s.wss.Stop(); err != nil {
		log.Errorln(err)
	}

	s.sesLock.Lock()
	for _, c := range s.clients {
		if c.session != nil {
			c.session.end(model.ServerShuttingDown)
		}
	}
	s.sesLock.Unlock()

	s.cancel()
	s.pubs.NotifyDispatcher()
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

	var lc net.ListenConfig
	l, err := lc.Listen(s.ctx, "tcp", s.TCP.Address)
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

func (s *Server) startDispatcher(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}

			log.Errorln("failed to accept connection:", err)
			return
		}

		go s.startSession(conn)
	}
}

// returns true if session is present and if state is reused.
func (s *Server) addSession(ses *session) bool {
	log.WithFields(log.Fields{
		"ClientId":     ses.clientId,
		"MQTT version": ses.protoVersion,
	}).Info("New client connected")

	sendSP := false

	s.sesLock.Lock()
	c, ok := s.clients[ses.clientId] // v4[MQTT-3.1.2-4]
	if ok {
		c.session.end(model.SessionTakenOver)

		if ses.cleanStart() || // v4[MQTT-3.1.2-6], v5[MQTT-3.1.2-4]
			(c.session.protoVersion < 5 && c.session.cleanStart()) ||
			(c.session.protoVersion > 4 && c.session.expiryInterval == 0) {

			s.removeClientSubscriptions(c)
			c.clearState()
		} else {
			sendSP = true
		}

		c.replaceSession(ses)
	} else {
		s.clients[ses.clientId] = newClient(ses)
		c = s.clients[ses.clientId]
	}
	ses.client = c
	s.sesLock.Unlock()

	if ok {
		log.WithFields(log.Fields{
			"ClientId": ses.clientId,
		}).Debug("Old session present")
	}
	if sendSP {
		log.WithFields(log.Fields{
			"ClientId": ses.clientId,
		}).Debug("New connection inheriting previous session state")

		if ses.protoVersion < 4 {
			sendSP = false
		}
	}

	return sendSP
}

func (s *Server) removeSession(ses *session) {
	s.sesLock.Lock()

	// check if another new session has not taken over already
	c, ok := s.clients[ses.clientId]
	if !ok || c.session != ses {
		s.sesLock.Unlock()
		return
	}

	s.removeClientSubscriptions(ses.client)
	ses.client.clearState()
	delete(s.clients, ses.clientId)

	s.sesLock.Unlock()

	log.WithFields(log.Fields{
		"ClientId": ses.clientId,
	}).Debug("client session removed")
}

// Subscription Options
func maxQoS(so uint8) uint8 {
	return so & 3
}
func noLocal(so uint8) bool {
	return so&4 > 0
}
func retainAsPublished(so uint8) bool {
	return so&8 > 0
}
func retainHandling(so uint8) uint8 {
	return so >> 4
}

type subscription struct {
	id      uint32
	options uint8
}

type sharedSubscriber struct {
	c   *client
	sub subscription
}

type sharedSub struct {
	subs []sharedSubscriber
	next int
}

func (ss *sharedSub) removeSubber(c *client) {
	for i := range ss.subs {
		if ss.subs[i].c == c {
			//ss.subs = append(ss.subs[:i], ss.subs[i+1:]...)
			li := len(ss.subs) - 1
			ss.subs[i] = ss.subs[li]
			ss.subs[li].c = nil
			ss.subs = ss.subs[:li]
			return
		}
	}
}

type topicLevel struct {
	children    topicTree
	subscribers map[*client]subscription
	shared      map[string]*sharedSub
}

func (tl *topicLevel) init() {
	tl.children = make(topicTree)
	tl.subscribers = make(map[*client]subscription)
	tl.shared = make(map[string]*sharedSub)
}

type topicTree map[string]*topicLevel // level -> sub levels

// Remove all subscriptions of client.
func (s *Server) removeClientSubscriptions(c *client) {
	var unSub func(topicTree, topT)
	unSub = func(sLevel topicTree, cLevel topT) {
		for l, cTL := range cLevel {
			sTL := sLevel[l]

			for gn := range cTL.sharedGroups {
				sg := sTL.shared[gn]
				sg.removeSubber(c)
				if len(sg.subs) == 0 {
					delete(sTL.shared, gn)
				}
				delete(cTL.sharedGroups, gn)
			}
			if cTL.subscribed {
				delete(sTL.subscribers, c)
				cTL.subscribed = false
			}
			unSub(sTL.children, cTL.children)
		}
	}

	s.subLock.Lock()
	unSub(s.subscriptions, c.subscriptions)
	s.subLock.Unlock()
}

// Add subscriptions for client. Also check for matching retained messages.
func (s *Server) addSubscriptions(c *client, topics [][][]byte, ops []uint8, id uint32) error {
	s.subLock.Lock()
	defer s.subLock.Unlock()

	for i, t := range topics {
		sLev, cLev := s.subscriptions, c.subscriptions

		var shareName string
		if bytes.Equal(t[0], isSharedSub) {
			if err := c.session.checkSharedSubscriptionTopicFilter(t); err != nil {
				return errors.New("malformed SUBSCRIBE: " + err.Error())
			}

			shareName = string(t[1])
			t = t[2:]
		}

		var sTL *topicLevel
		var cTL *topL
		var ok bool
		for _, tl := range t {
			tlStr := bytesToStringUnsafe(tl)
			// Server subscriptions
			if sTL, ok = sLev[tlStr]; !ok {
				sTL = &topicLevel{}
				sLev[string(tl)] = sTL
				sTL.init()
			}

			// Client's subscriptions
			if cTL, ok = cLev[tlStr]; !ok {
				cTL = &topL{}
				cLev[string(tl)] = cTL
				cTL.children = make(topT)
				cTL.sharedGroups = make(map[string]struct{})
			}

			sLev, cLev = sTL.children, cTL.children
		}

		sub := subscription{id, ops[i]}
		if len(shareName) == 0 {
			sTL.subscribers[c] = sub
		} else {
			ss := sTL.shared[shareName]
			if ss == nil {
				ss = &sharedSub{subs: make([]sharedSubscriber, 1, 4)}
				sTL.shared[shareName] = ss
				ss.subs[0] = sharedSubscriber{c, sub}
			} else {
				var modified bool
				for j := range ss.subs {
					if ss.subs[j].c == c {
						ss.subs[j].sub = sub
						modified = true
						break
					}
				}
				if !modified {
					ss.subs = append(ss.subs, sharedSubscriber{c, sub})
				}
			}

			cTL.sharedGroups[shareName] = struct{}{}
			continue // TODO: send retained messages if sub already exists?
		}

		subDoesExist := cTL.subscribed
		cTL.subscribed = true

		// Retained messages
		if rh := retainHandling(ops[i]); rh == 2 || (rh == 1 && subDoesExist) {
			continue
		}

		forwardLevel := func(l *retainLevel) {
			if l.p != nil {
				c.processPub(l.p, sub, true)
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
			tlStr := bytesToStringUnsafe(t[n])
			switch tlStr {
			case "#":
				forwardAll(l)
			case "+":
				switch len(t) - n {
				case 1:
					for _, nl := range l {
						forwardLevel(nl)
					}
				case 2:
					if t[len(t)-1][0] == '#' {
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
				nl, ok := l[tlStr]
				if !ok {
					break
				}

				switch len(t) - n {
				case 1:
					forwardLevel(nl)
				case 2:
					if t[len(t)-1][0] == '#' {
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
	return nil
}

func (s *Server) removeSubscriptions(c *client, topics [][][]byte) error {
	var sTL *topicLevel
	var cTL *topL
	var ok bool

	s.subLock.Lock()
	defer s.subLock.Unlock()

loop:
	for _, t := range topics {
		sl, cl := s.subscriptions, c.subscriptions

		var shareName string
		if bytes.Equal(t[0], isSharedSub) {
			if err := c.session.checkSharedSubscriptionTopicFilter(t); err != nil {
				return errors.New("malformed UNSUBSCRIBE: " + err.Error())
			}

			shareName = bytesToStringUnsafe(t[1])
			t = t[2:]
		}

		for _, tl := range t {
			tlStr := bytesToStringUnsafe(tl)
			// Server
			if sTL, ok = sl[tlStr]; !ok {
				continue loop // no one subscribed to this
			}

			// Client
			if cTL, ok = cl[tlStr]; !ok {
				continue loop // client not subscribed
			}

			sl, cl = sTL.children, cTL.children
		}

		if len(shareName) == 0 {
			delete(sTL.subscribers, c)
			cTL.subscribed = false
		} else {
			ss := sTL.shared[shareName]
			if ss != nil {
				ss.removeSubber(c)
				if len(ss.subs) == 0 {
					delete(sTL.shared, shareName)
				}
			}
			delete(cTL.sharedGroups, shareName)
		}

	}
	return nil
}

func (s *Server) forwardToSubscribers(tl *topicLevel, p *model.PubMessage) {
	// Shared Subscriptions
	// TODO: better algo: check conn health, limit to QoS1, queue, etc.
	for _, ss := range tl.shared {
		subber := ss.subs[ss.next%len(ss.subs)]
		ss.next++
		if ss.next < 0 {
			ss.next = 0
		}
		subber.c.processPub(p, subber.sub, false)
	}

	// Non-shared Subscriptions
	for c, sub := range tl.subscribers {
		c.processPub(p, sub, false)
	}
}

func (s *Server) matchTopicLevel(p *model.PubMessage, l topicTree, ln int) {
	// direct match
	if nl, ok := l[bytesToStringUnsafe(s.sepPubTopic[ln])]; ok {
		if ln < len(s.sepPubTopic)-1 {
			s.matchTopicLevel(p, nl.children, ln+1)
		} else {
			s.forwardToSubscribers(nl, p)
			if nl, ok := nl.children["#"]; ok { // # match - next level
				s.forwardToSubscribers(nl, p)
			}
		}
	}

	if ln == 0 && len(s.sepPubTopic[0]) > 0 && s.sepPubTopic[0][0] == '$' {
		return // [MQTT-4.7.2-1]
	}

	// # match
	if nl, ok := l["#"]; ok {
		s.forwardToSubscribers(nl, p)
	}

	// + match
	if nl, ok := l["+"]; ok {
		if ln < len(s.sepPubTopic)-1 {
			s.matchTopicLevel(p, nl.children, ln+1)
		} else {
			s.forwardToSubscribers(nl, p)
			if nl, ok := nl.children["#"]; ok { // # match - next level
				s.forwardToSubscribers(nl, p)
			}
		}
	}
}

// Match published message topic to all subscribers, and forward.
// Also store pub if retained message.
func (s *Server) matchSubscriptions(i *queue.Item) error {
	tLen := binary.BigEndian.Uint16(i.P.B[1:])
	s.splitPubTopic(i.P.B[3 : 3+tLen])

	s.subLock.RLock()
	s.matchTopicLevel(i.P, s.subscriptions, 0)

	if !i.P.ToRetain() {
		s.subLock.RUnlock()
		i.P.FreeIfLastUser()
		return nil
	}

	tr := s.retained
	var nl *retainLevel
	var ok bool
	for _, tl := range s.sepPubTopic {
		if nl, ok = tr[bytesToStringUnsafe(tl)]; !ok {
			nl = &retainLevel{children: make(retainTree)}
			tr[string(tl)] = nl
		}

		tr = nl.children
	}

	oldRetained := nl.p

	if len(i.P.B) == 3+int(tLen) {
		// payload empty, so delete existing retained message
		nl.p = nil
		s.subLock.RUnlock()
		i.P.FreeIfLastUser() // free as not going to be used
	} else {
		nl.p = i.P
		s.subLock.RUnlock()
		// do not free p as we transfer ref of this thread to nl.p
	}

	if oldRetained != nil {
		oldRetained.FreeIfLastUser()
	}

	return nil
}

type retainLevel struct {
	p        *model.PubMessage
	children retainTree
}

type retainTree map[string]*retainLevel

// DO NOT use result for making an entry in map
func bytesToStringUnsafe(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (s *Server) splitPubTopic(topic []byte) {
	s.sepPubTopic = s.sepPubTopic[:0]
	for i := bytes.IndexByte(topic, '/'); i >= 0; i = bytes.IndexByte(topic, '/') {
		s.sepPubTopic = append(s.sepPubTopic, topic[:i])
		topic = topic[i+1:]
	}
	s.sepPubTopic = append(s.sepPubTopic, topic)
}

// returns readable UUID (non-standard)
// try 128-bit random number. If cannot, use current time.
func generateRandomID() []byte {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		log.Warnln("failed to generate random UUID: ", err)
		return strconv.AppendInt(make([]byte, 0, 19), time.Now().UnixNano(), 10)
	}

	id := make([]byte, 36)
	hex.Encode(id, b[:4])
	id[8] = '-'
	hex.Encode(id[9:], b[4:6])
	id[13] = '-'
	hex.Encode(id[14:], b[6:8])
	id[18] = '-'
	hex.Encode(id[19:], b[8:10])
	id[23] = '-'
	hex.Encode(id[24:], b[10:])

	return id
}
