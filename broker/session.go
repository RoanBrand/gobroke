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

	will     pub
	userName string
	password []byte
}

func (s *session) run() {
	go s.startWriter()

	go s.qos0Pump()
	go s.qos1Pump()
	go s.qos2Pump()
}

func (s *session) stop() {
	s.onlyOnce.Do(func() {
		s.conn.Close()
		c := s.client
		if c != nil {
			c.q0Lock.Lock()
			c.q1Lock.Lock()
			c.q2Lock.Lock()
			c.txLock.Lock()
		}
		s.dead = true
		if c != nil {
			c.txFlush <- struct{}{}
			c.txLock.Unlock()
			c.q2Trig.Signal()
			c.q2Lock.Unlock()
			c.q1Trig.Signal()
			c.q1Lock.Unlock()
			c.q0Trig.Signal()
			c.q0Lock.Unlock()
		}
	})
}

func (s *session) qos0Pump() {
	c := s.client
	defer c.q0Lock.Unlock()

	for {
		c.q0Lock.Lock()
		if s.dead {
			return
		}

		if c.q0Q.Front() == nil {
			c.q0Trig.Wait()
		}
		if s.dead {
			return
		}

		for p0 := c.q0Q.Front(); p0 != nil; p0 = p0.Next() {
			if err := s.writePacket(p0.Value.([]byte)); err != nil {
				return
			}
			c.q0Q.Remove(p0)
		}
		c.q0Lock.Unlock()
	}
}

func setDUPFlag(pub []byte) {
	pub[0] |= 0x08
}

func (s *session) qos1Pump() {
	c := s.client
	started := false
	defer c.q1Lock.Unlock()

	for {
		c.q1Lock.Lock()
		if s.dead {
			return
		}

		if c.q1Q.Front() == nil {
			c.q1Trig.Wait()
		}
		if s.dead {
			return
		}

		for p1 := c.q1Q.Front(); p1 != nil; p1 = p1.Next() {
			p := p1.Value.(*qosPub)
			if started {
				if p.sent {
					continue
				}
			} else if p.sent {
				p.p[0] |= 0x08 // DUP
				setDUPFlag(p.p)
			}
			if err := s.writePacket(p.p); err != nil {
				return
			}
			p.sent = true
			go s.qosMonitor(p, setDUPFlag)
		}
		c.q1Lock.Unlock()
		started = true
	}
}

func (s *session) qos2Pump() {
	c := s.client
	started := false
	defer c.q2Lock.Unlock()

	for {
		c.q2Lock.Lock()
		if s.dead {
			return
		}

		if c.q2Q.Front() == nil {
			c.q2Trig.Wait()
		}
		if s.dead {
			return
		}

		for p2 := c.q2Q.Front(); p2 != nil; p2 = p2.Next() {
			p := p2.Value.(*qosPub)
			if started {
				if p.sent {
					continue
				}
			} else if p.sent {
				setDUPFlag(p.p)
			}
			if err := s.writePacket(p.p); err != nil {
				return
			}
			p.sent = true
			go s.qosMonitor(p, setDUPFlag)
		}
		c.q2Lock.Unlock()
		started = true
	}
}

func (s *session) qosMonitor(pub *qosPub, preResend func([]byte)) {
	t := time.NewTimer(time.Second * 20) // correct value or method?
	for {
		select {
		case <-pub.done:
			if !t.Stop() {
				<-t.C
			}
			return
		case <-t.C:
			if preResend != nil {
				preResend(pub.p)
			}
			if err := s.writePacket(pub.p); err != nil {
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
