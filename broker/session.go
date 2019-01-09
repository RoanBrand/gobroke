package broker

import (
	"errors"
	"net"
	"time"

	log "github.com/sirupsen/logrus"
)

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
	p := []byte{CONNACK, 2, sp, errCode}
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
			log.WithFields(log.Fields{
				"id":  s.clientId,
				"err": err,
			}).Error("TCP TX error")
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
