package broker

import (
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

type session struct {
	client *client

	conn    net.Conn
	packet  packet
	rxState uint8

	dead     int32
	onlyOnce sync.Once

	clientId        string
	connectSent     bool
	notFirstSession bool
	connectFlags    byte
	keepAlive       time.Duration

	will     pub
	userName string
	password []byte
}

func (s *session) run() {
	go s.startWriter()

	go s.client.q0.StartDispatcher(s.writePacket, &s.dead)
	go s.client.q1.StartDispatcher(s.writePacket, &s.dead)
	s.client.q2Stage2.ResendAll(s.writePacket)
	go s.client.q2.StartDispatcher(s.writePacket, &s.dead)
}

func (s *session) stop() {
	s.onlyOnce.Do(func() {
		s.conn.Close()
		atomic.StoreInt32(&s.dead, 1)
		if c := s.client; c != nil {
			c.txFlush <- struct{}{}
			c.q0.Signal()
			c.q1.Signal()
			c.q2.Signal()
		}
	})
}

func (s *session) updateTimeout() {
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

func (s *session) writePacket(p []byte) error {
	s.client.txLock.Lock()
	if _, err := s.client.tx.Write(p); err != nil {
		s.client.txLock.Unlock()
		return err
	}

	if len(s.client.txFlush) == 0 {
		select {
		case s.client.txFlush <- struct{}{}:
		default:
		}
	}

	s.client.txLock.Unlock()
	return nil
}

func (s *Server) startSession(conn net.Conn) {
	conn.SetReadDeadline(time.Now().Add(time.Second * 20)) // CONNECT packet timeout
	ns := session{conn: conn}
	ns.packet.vh = make([]byte, 0, 512)
	ns.packet.payload = make([]byte, 0, 512)

	graceFullExit := false
	defer func() {
		if !graceFullExit && ns.will.topic != "" && ns.connectSent {
			s.pubs <- ns.will
		}
		ns.stop()
	}()

	rx := make([]byte, 1024)
	for {
		n, err := conn.Read(rx)
		if err != nil {
			errStr := err.Error()
			if errStr == "EOF" {
				log.Println("client closed connection gracefully")
				if !ns.persistent() { // [MQTT-3.1.2-6]
					s.unregister <- &ns
				}
				return
			}

			if strings.Contains(errStr, "use of closed") {
				return
			}

			// KeepAlive timeout
			if strings.Contains(errStr, "i/o timeout") {
				l := log.WithFields(log.Fields{
					"client": ns.clientId,
				})
				if ns.connectSent {
					l.Debug("KeepAlive timeout. Dropping connection")
				} else {
					l.Debug("Timeout waiting for CONNECT. Dropping connection")
				}
				return
			}

			log.WithFields(log.Fields{
				"client": ns.clientId,
				"err":    err,
			}).Error("TCP RX error")
			return
		}

		if n > 0 {
			if err := s.parseStream(&ns, rx[:n]); err != nil {
				if err == errCleanExit {
					graceFullExit = true
				} else {
					log.WithFields(log.Fields{
						"client": ns.clientId,
						"err":    err,
					}).Error("RX stream packet parse error")
				}
				return
			}
		}
	}
}

func (s *session) startWriter() {
	for range s.client.txFlush {
		s.client.txLock.Lock()
		if atomic.LoadInt32(&s.dead) == 1 {
			s.client.txLock.Unlock()
			return
		}

		if s.client.tx.Buffered() > 0 {
			if err := s.client.tx.Flush(); err != nil {
				if strings.Contains(err.Error(), "use of closed") {
					s.client.txLock.Unlock()
					return
				}

				log.WithFields(log.Fields{
					"client": s.clientId,
					"err":    err,
				}).Error("TCP TX error")
				s.client.txLock.Unlock()
				return
			}
		}
		s.client.txLock.Unlock()
	}
}
