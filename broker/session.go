package broker

import (
	"net"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type session struct {
	client *client

	conn    net.Conn
	packet  packet
	rxState uint8

	dead     bool
	onlyOnce sync.Once

	clientId        string
	connectSent     bool
	notFirstSession bool
	connectFlags    byte
	keepAlive       time.Duration
}

func (s *session) run() {
	// Writer
	go s.startWriter()

	// QoS 0
	go s.qos0Pump()

	// QoS 1
	go s.qos1Pump()
}

func (s *session) stop() {
	s.onlyOnce.Do(func() {
		s.conn.Close()
		c := s.client
		if c != nil {
			c.q0Lock.Lock()
			c.q1Lock.Lock()
			c.txLock.Lock()
		}
		s.dead = true
		if c != nil {
			c.txFlush <- struct{}{}
			c.txLock.Unlock()
			c.q1Cond.Signal()
			c.q1Lock.Unlock()
			c.q0Cond.Signal()
			c.q0Lock.Unlock()
		}
	})
}

func (s *session) qos0Pump() {
	c := s.client
	for {
		c.q0Lock.Lock()
		if s.dead {
			c.q0Lock.Unlock()
			return
		}

		if c.q0Q.Front() == nil {
			c.q0Cond.Wait()
		}
		if s.dead {
			c.q0Lock.Unlock()
			return
		}

		for p0 := c.q0Q.Front(); p0 != nil; p0 = p0.Next() {
			if err := s.writePacket(p0.Value.([]byte)); err != nil {
				c.q0Lock.Unlock()
				return
			}
			c.q0Q.Remove(p0)
		}
		c.q0Lock.Unlock()
	}
}

func (s *session) qos1Pump() {
	c := s.client
	started := false
	for {
		c.q1Lock.Lock()
		if s.dead {
			c.q1Lock.Unlock()
			return
		}

		if c.q1Q.Front() == nil {
			c.q1Cond.Wait()
		}
		if s.dead {
			c.q1Lock.Unlock()
			return
		}

		for p1 := c.q1Q.Front(); p1 != nil; p1 = p1.Next() {
			p := p1.Value.(*qosPub)
			if started {
				if p.sent {
					continue
				}
			} else {
				if p.sent {
					p.p[0] |= 0x08 // DUP
				}
			}
			if err := s.writePacket(p.p); err != nil {
				c.q1Lock.Unlock()
				return
			}
			p.sent = true
			go s.qos1Monitor(p)
		}
		c.q1Lock.Unlock()
		started = true
	}
}

func (s *session) qos1Monitor(pub1 *qosPub) {
	t := time.NewTimer(time.Second * 20) // correct value or method?
	for {
		select {
		case <-pub1.done:
			if !t.Stop() {
				<-t.C
			}
			return
		case <-t.C:
			pub1.p[0] |= 0x08 // DUP
			if err := s.writePacket(pub1.p); err != nil {
				return
			}
			t.Reset(time.Second * 20)
		}
	}
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
	defer ns.stop()

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
				log.WithFields(log.Fields{
					"client": ns.clientId,
					"err":    err,
				}).Error("RX stream packet parse error")
				return
			}
		}
	}
}

func (s *session) startWriter() {
	for {
		select {
		case <-s.client.txFlush:
			s.client.txLock.Lock()
			if s.dead {
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
}
