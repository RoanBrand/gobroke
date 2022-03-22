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
	dead2    chan struct{}
	onlyOnce sync.Once
	stopped  sync.WaitGroup

	clientId     string
	connectSent  bool
	connectFlags byte
	keepAlive    time.Duration

	will     pub
	userName string
	password []byte
}

func (s *session) run(retryInterval uint64) {
	s.stopped.Add(1)
	go s.startWriter()
	c := s.client

	if err := c.q2Stage2.ResendAll(s.writePacket); err != nil {
		log.WithFields(log.Fields{
			"client": s.clientId,
			"err":    err,
		}).Error("Unable to resend all pending QoS2 PUBRELs")
	}
	if err := c.q2.ResendAll(s.writePacket); err != nil {
		log.WithFields(log.Fields{
			"client": s.clientId,
			"err":    err,
		}).Error("Unable to resend all pending QoS2 PUBLISHs")
	}
	if err := c.q1.ResendAll(s.writePacket); err != nil {
		log.WithFields(log.Fields{
			"client": s.clientId,
			"err":    err,
		}).Error("Unable to resend all pending QoS1 PUBLISHs")
	}

	s.stopped.Add(3)
	go c.q0.StartDispatcher(s.writePacket, &s.dead, &s.stopped)
	go c.q1.StartDispatcher(s.writePacket, &s.dead, &s.stopped)
	go c.q2.StartDispatcher(s.writePacket, &s.dead, &s.stopped)
	if retryInterval > 0 {
		s.stopped.Add(3)
		go c.q2Stage2.MonitorTimeouts(retryInterval, s.writePacket, s.dead2, &s.stopped)
		go c.q1.MonitorTimeouts(retryInterval, s.writePacket, s.dead2, &s.stopped)
		go c.q2.MonitorTimeouts(retryInterval, s.writePacket, s.dead2, &s.stopped)
	}
}

func (s *session) stop() {
	s.onlyOnce.Do(func() {
		atomic.StoreInt32(&s.dead, 1)
		close(s.dead2)
		s.conn.Close()
		if c := s.client; c != nil {
			c.txLock.Lock()
			if len(c.txFlush) == 0 {
				select {
				case c.txFlush <- struct{}{}:
				default:
				}
			}
			c.txLock.Unlock()
			c.q0.Signal()
			c.q1.Signal()
			c.q2.Signal()
		}
		s.stopped.Wait()
	})
}

func (s *session) updateTimeout() {
	if s.keepAlive > 0 {
		s.conn.SetReadDeadline(time.Now().Add(s.keepAlive))
	}
}

// Send CONNACK with optional Session Present flag.
func (s *session) sendConnack(errCode uint8, SP bool) error {
	p := []byte{CONNACK, 2, 0, errCode}
	if SP {
		p[2] = 1
	}
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
	conn.SetReadDeadline(time.Now().Add(time.Second * 10)) // CONNECT packet timeout
	ns := session{conn: conn, dead2: make(chan struct{})}
	ns.packet.vh = make([]byte, 0, 512)
	ns.packet.payload = make([]byte, 0, 512)

	ns.stopped.Add(1)
	graceFullExit := false
	defer func() {
		ns.stopped.Done()
		ns.stop()
		if ns.connectSent {
			if !ns.persistent() { // [MQTT-3.1.2-6]
				s.removeSession(&ns)
			}
			if !graceFullExit && len(ns.will.topic) != 0 {
				s.pubs <- ns.will
			}
		}
	}()

	rx := make([]byte, 1024)
	for {
		n, err := conn.Read(rx)
		if err != nil {
			errStr := err.Error()
			if errStr == "EOF" {
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
	defer s.stopped.Done()
	for range s.client.txFlush {
		s.client.txLock.Lock()
		if atomic.LoadInt32(&s.dead) == 1 {
			s.client.txLock.Unlock()
			return
		}

		if s.client.tx.Buffered() > 0 {
			if err := s.client.tx.Flush(); err != nil {
				s.client.txLock.Unlock()
				if strings.Contains(err.Error(), "use of closed") {
					return
				}
				log.WithFields(log.Fields{
					"client": s.clientId,
					"err":    err,
				}).Error("TCP TX error")
				return
			}
		}
		s.client.txLock.Unlock()
	}
}
