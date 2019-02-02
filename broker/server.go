package broker

import (
	"crypto/tls"
	"encoding/binary"
	"io/ioutil"
	"net"
	"os"

	"github.com/RoanBrand/gobroke/config"
	log "github.com/sirupsen/logrus"
)

const (
	serverBuffer = 1024
)

type server struct {
	config        *config.Config
	clients       map[string]*client
	subscriptions map[string]map[string]uint8 // topic -> subscribed client id -> QoS level

	register   chan *session
	unregister chan *session
	subs       chan *subList
	pubs       chan *pub
}

func NewServer(confPath string) (*server, error) {
	conf, err := config.New(confPath)
	if err != nil {
		return nil, err
	}

	return &server{
		config:        conf,
		clients:       make(map[string]*client, 16),
		subscriptions: make(map[string]map[string]uint8, 4),
		register:      make(chan *session),
		unregister:    make(chan *session, 32),
		subs:          make(chan *subList),
		pubs:          make(chan *pub, serverBuffer),
	}, nil
}

func (s *server) Start() error {
	er := make(chan error)

	if err := s.setupTCP(er); err != nil {
		return err
	}
	if err := s.setupTLS(er); err != nil {
		return err
	}

	go s.run(er)
	return <-er
}

func (s *server) run(chan error) {
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
		case c := <-s.unregister:
			s.removeClient(c.clientId)
		case sl := <-s.subs:
			s.addSubscriptions(sl)
		case p := <-s.pubs:
			s.forwardToSubscribers(p)
		}
	}
}

func (s *server) setupTCP(errPipe chan error) error {
	if !s.config.TCP.Enabled {
		return nil
	}

	l, err := net.Listen("tcp", s.config.TCP.Address)
	if err != nil {
		return err
	}

	go func(l net.Listener) {
		for {
			conn, err := l.Accept()
			if err != nil {
				errPipe <- err
				return
			}

			go s.startSession(conn)
		}
	}(l)

	return nil
}

func (s *server) setupTLS(errPipe chan error) error {
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

	go func(l net.Listener) {
		for {
			conn, err := l.Accept()
			if err != nil {
				errPipe <- err
				return
			}

			go s.startSession(conn)
		}
	}(l)

	return nil
}

func (s *server) addSession(newses *session) {
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

func (s *server) removeClient(id string) {
	log.WithFields(log.Fields{
		"client": id,
	}).Debug("Deleting client session (CleanSession)")
	delete(s.clients, id)
}

type subList struct {
	cId    string
	topics []string
	qoss   []uint8
}

func (s *server) addSubscriptions(subs *subList) {
	for i, t := range subs.topics {
		cl, ok := s.subscriptions[t]
		if !ok {
			s.subscriptions[t] = make(map[string]uint8, 8)
			cl = s.subscriptions[t]
		}

		cl[subs.cId] = subs.qoss[i]
	}
}

type pub struct {
	topic  string
	pacs   [][]byte
	pubQoS uint8
	idLoc  int
}

func (s *server) publishToSubscribers(p *packet, from string) {
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

	s.pubs <- &pub{topic: topic, pacs: pacs, pubQoS: qos, idLoc: idLoc}
}

func (s *server) forwardToSubscribers(p *pub) {
	for cId := range s.subscriptions[p.topic] {
		if c, ok := s.clients[cId]; ok {
			c.pubRX <- p
		}
	}
}
