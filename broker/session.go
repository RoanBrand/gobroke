package broker

import (
	"bufio"
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

	publishId uint16
	qos1Queue map[uint16]chan<- struct{} // used to send done signal
	qLock     sync.Mutex
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

func (s *session) persistent() bool {
	return s.connectFlags&0x02 == 0
}

func (s *session) sendQoS1Message(pubP []byte, pubId uint16) {
	done := make(chan struct{})
	s.qLock.Lock()
	s.qos1Queue[s.publishId] = done
	s.qLock.Unlock()

	s.writePacket(pubP)

	for {
		select {
		case <-done:
			return
		case <-time.After(time.Second * 20): // what should this be?
			pubP[1] |= 0x08 // DUP
			if err := s.writePacket(pubP); err != nil {
				return
			}
		}
	}
}

func (s *session) signalQoS1Done(pubId uint16) {
	s.qLock.Lock()
	done, ok := s.qos1Queue[pubId]
	if ok {
		close(done)
		delete(s.qos1Queue, pubId)
	} else {
		log.WithFields(log.Fields{
			"client":   s.clientId,
			"packetID": pubId,
		}).Error("Got PUBACK packet for none existing packet")
	}
	s.qLock.Unlock()
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
