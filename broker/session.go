package broker

import (
	"bufio"
	"errors"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type session struct {
	conn    net.Conn
	tx      *bufio.Writer
	txLock  sync.Mutex
	txFlush chan struct{}

	packet  packet
	rxState uint8

	sentConnectPacket bool
	notFirstSession   bool
	connectFlags      byte
	keepAlive         time.Duration

	clientId      string
	subscriptions map[string]struct{} // unused. list of channels
	// QoS 1 & 2 unacknowledged and pending
}

func (s *session) close() {
	s.conn.Close()
}

func (s *session) setDeadline() {
	if s.keepAlive > 0 {
		s.conn.SetReadDeadline(time.Now().Add(s.keepAlive))
	}
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
	for range s.txFlush {
		s.txLock.Lock()
		if s.tx.Buffered() > 0 {
			if err := s.tx.Flush(); err != nil {
				log.WithFields(log.Fields{
					"id":  s.clientId,
					"err": err,
				}).Error("TCP TX error")
				s.txLock.Unlock()
				return
			}
		}
		s.txLock.Unlock()
	}
}

func (s *session) writePacket(p []byte) error {
	s.txLock.Lock()
	if _, err := s.tx.Write(p); err != nil {
		s.txLock.Unlock()
		return err
	}

	if len(s.txFlush) == 0 {
		select {
		case s.txFlush <- struct{}{}:
		default:
		}
	}

	s.txLock.Unlock()
	return nil
}
