package broker_test

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RoanBrand/gobroke/broker"
	"github.com/sirupsen/logrus"
)

func TestRejoin(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	s, err := broker.NewServer("config.json")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	errPipe := make(chan error)
	go func() {
		if err := s.Start(); err != nil {
			errPipe <- err
		}
	}()

	var sp1, sp2 uint8
	var c1PubReadId uint16 = 1
	gotPubID := false

	for i := 0; i < 500; i++ {
		c1, err := doConnect('1', false, sp1)
		if err != nil {
			t.Fatal(err)
		}
		sp1 = 1
		err = c1.sub("t", 1)
		if err != nil {
			t.Fatal(err)
		}

		c2, err := doConnect('2', false, sp2)
		if err != nil {
			t.Fatal(err)
		}
		sp2 = 1

		err = c2.pubMsg([]byte("MSG"), "t", 1)
		if err != nil {
			t.Fatal(err)
		}

		pub := <-c1.pubs
		if !gotPubID {
			c1PubReadId = pub.pID
			gotPubID = true
		}
		if pub.topic != "t" || pub.dup || pub.qos != 1 || pub.pID != c1PubReadId || string(pub.msg) != "MSG" {
			t.Fatal("got pub:", pub, "pID must be:", c1PubReadId, "")
		}
		if err := c1.sendPuback(pub.pID); err != nil {
			t.Fatal(err)
		}
		c1PubReadId++

		if i%2 == 0 {
			c1.conn.Close()
		}

		c1, err = doConnect('1', false, sp1)
		if err != nil {
			t.Fatal(err)
		}

		err = c2.pubMsg([]byte("MSG"), "t", 1)
		if err != nil {
			t.Fatal(err)
		}

		pub = <-c1.pubs
		if pub.topic != "t" || pub.dup || pub.qos != 1 || pub.pID != c1PubReadId || string(pub.msg) != "MSG" {
			t.Fatal("got pub:", pub, "pID must be:", c1PubReadId, "")
		}
		if err := c1.sendPuback(pub.pID); err != nil {
			t.Fatal(err)
		}
		c1PubReadId++

	last:
		for {
			select {
			case pub := <-c1.pubs:
				t.Fatal("unknown pub received:", pub, string(pub.msg))
			case pub := <-c2.pubs:
				t.Fatal("unknown pub received:", pub, string(pub.msg))
			case <-time.After(time.Microsecond * 10):
				break last
			case err := <-errPipe:
				t.Fatal(err)
			}

		}
	}

	_, err = doConnect('1', true, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = doConnect('2', true, 0)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errPipe:
		t.Fatal(err)
	default:
	}
}

func TestResendPendingQos1AtReconnect(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	s, err := broker.NewServer("config.json")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	errPipe := make(chan error)
	go func() {
		if err := s.Start(); err != nil {
			errPipe <- err
		}
	}()

	// startup
	c1, err := doConnect('1', false, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = c1.sub("t", 1)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := doConnect('2', false, 0)
	if err != nil {
		t.Fatal(err)
	}

	// 1 with puback
	err = c2.pubMsg([]byte("msg1"), "t", 1)
	if err != nil {
		t.Fatal(err)
	}

	pub := <-c1.pubs
	if pub.qos != 1 || pub.topic != "t" || string(pub.msg) != "msg1" || pub.dup {
		t.Fatalf("got pub: %+v %s\n", pub, string(pub.msg))
	}
	if err := c1.sendPuback(pub.pID); err != nil {
		t.Fatal(err)
	}

	// 2,3 without puback
	err = c2.pubMsg([]byte("msg2"), "t", 1)
	if err != nil {
		t.Fatal(err)
	}
	pub = <-c1.pubs
	if pub.qos != 1 || pub.topic != "t" || string(pub.msg) != "msg2" || pub.dup {
		t.Fatalf("got pub: %+v\n", pub)
	}

	err = c2.pubMsg([]byte("msg2"), "t", 3)
	if err != nil {
		t.Fatal(err)
	}
	pub = <-c1.pubs
	if pub.qos != 1 || pub.topic != "t" || string(pub.msg) != "msg3" || pub.dup {
		t.Fatalf("got pub: %+v\n", pub)
	}

	c1.conn.Close()
	time.Sleep(time.Microsecond * 200) // ensure c1 is closed before server sends 4 to it (might still get 4 as dup)

	// 4, 5 no receive
	err = c2.pubMsg([]byte("msg4"), "t", 1)
	if err != nil {
		t.Fatal(err)
	}
	err = c2.pubMsg([]byte("msg5"), "t", 1)
	if err != nil {
		t.Fatal(err)
	}

	c1, err = doConnect('1', false, 1)
	if err != nil {
		t.Fatal(err)
	}

	// receive 2,3,4,5
	pub = <-c1.pubs
	if pub.qos != 1 || pub.topic != "t" || string(pub.msg) != "msg2" || !pub.dup {
		t.Fatalf("got pub: %+v\n", pub)
	}
	if err := c1.sendPuback(pub.pID); err != nil {
		t.Fatal(err)
	}

	pub = <-c1.pubs
	if pub.qos != 1 || pub.topic != "t" || string(pub.msg) != "msg3" || !pub.dup {
		t.Fatalf("got pub: %+v\n", pub)
	}
	if err := c1.sendPuback(pub.pID); err != nil {
		t.Fatal(err)
	}

	pub = <-c1.pubs // dup might be true, is fine. (nature of qos1)
	if pub.qos != 1 || pub.topic != "t" || string(pub.msg) != "msg4" {
		t.Fatalf("got pub: %+v\n", pub)
	}
	if err := c1.sendPuback(pub.pID); err != nil {
		t.Fatal(err)
	}

	pub = <-c1.pubs
	if pub.qos != 1 || pub.topic != "t" || string(pub.msg) != "msg5" || pub.dup {
		t.Fatalf("got pub: %+v\n", pub)
	}
	if err := c1.sendPuback(pub.pID); err != nil {
		t.Fatal(err)
	}

	s.Stop()
last:
	for {
		select {
		case pub := <-c1.pubs:
			t.Fatal("unknown pub received:", pub)
		case <-time.After(time.Microsecond * 200):
			break last
		}
	}

	select {
	case err := <-errPipe:
		t.Fatal(err)
	case err := <-c1.errs:
		// TODO: figure out
		if !strings.Contains(err.Error(), "An existing connection was forcibly closed by the remote host") {
			t.Fatal(err)
		}
	case err := <-c2.errs:
		t.Fatal(err)
	default:
	}
}

func BenchmarkQoS1(b *testing.B) {
	logrus.SetLevel(logrus.ErrorLevel)
	var qos uint8 = 0

	errPipe := make(chan error)
	s, err := broker.NewServer("config.json")
	if err != nil {
		b.Fatal(err)
	}
	defer s.Stop()
	go func() {
		if err := s.Start(); err != nil {
			errPipe <- err
		}
	}()

	c1, err := doConnect('1', true, 0)
	if err != nil {
		b.Fatal(err)
	}

	err = c1.sub("t", qos)
	if err != nil {
		b.Fatal(err)
	}

	c2, err := doConnect('2', true, 0)
	if err != nil {
		b.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		for i := 0; i < b.N; i++ {
			pub := <-c1.pubs
			if qos == 1 {
				if err = c1.sendPuback(pub.pID); err != nil {
					b.Fatal(err)
				}
			}
		}
		close(done)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = c2.pubMsg([]byte("msg"), "t", qos); err != nil {
			b.Fatal(err)
		}
	}
	<-done
	b.StopTimer()
	b.ReportAllocs()
}

type fakeClient struct {
	errs      chan error
	conn      net.Conn
	pubId     uint16
	pubs      chan pubInfo
	connected chan connackInfo
	subbed    chan subackInfo

	q1Lock    sync.Mutex
	q1Pubacks map[uint16]struct{} // pubs to server
	//q1Pubs    map[uint16]struct{} // pubs from server
}

type pubInfo struct {
	topic string
	dup   bool
	qos   uint8
	pID   uint16
	msg   []byte
}

type connackInfo struct {
	sp   uint8
	code uint8
}

type subackInfo struct {
	pID uint16
	qos uint8
}

func (c *fakeClient) reader() {
	rx := make([]byte, 256)
	var rxState, controlAndFlags uint8
	var remainLen, lenMul, vhLen uint32
	vh, payload := make([]byte, 0, 256), make([]byte, 0, 256)

	for {
		nRx, err := c.conn.Read(rx)
		if err != nil {
			if err.Error() != "EOF" && !strings.Contains(err.Error(), "use of closed") {
				c.errs <- err
			}
			return
		}

		i, l := uint32(0), uint32(len(rx[:nRx]))
		for i < l {
			switch rxState {
			case 0: // control & flags
				controlAndFlags = rx[i]
				lenMul, remainLen, rxState = 1, 0, 1
				i++
			case 1: // remaining len
				remainLen += uint32(rx[i]&127) * lenMul
				lenMul *= 128
				if rx[i]&128 == 0 {
					switch controlAndFlags & 0xF0 {
					case broker.PUBLISH:
						vhLen = 0
					default: // CONNACK, PUBACK, SUBACK
						vhLen = 2
					}

					if remainLen == 0 {
						rxState = 0
					} else {
						vh = vh[:0]
						if controlAndFlags&0xF0 == broker.PUBLISH {
							rxState = 2
						} else {
							rxState = 3
						}
					}
				}
				i++
			case 2: // variable header length
				vh = append(vh, rx[i])
				vhLen++
				remainLen--

				if vhLen == 2 {
					vhLen = uint32(binary.BigEndian.Uint16(vh))
					if controlAndFlags&0x06 > 0 {
						vhLen += 2
					}
					rxState = 3
				}
				i++
			case 3: // variable header
				avail := l - i
				toRead := vhLen
				if avail < toRead {
					toRead = avail
				}
				vh = append(vh, rx[i:i+toRead]...)
				vhLen -= toRead
				remainLen -= toRead

				if vhLen == 0 {
					switch controlAndFlags & 0xF0 {
					case broker.CONNACK:
						c.connected <- connackInfo{vh[0], vh[1]}
					case broker.PUBACK:
						pID := binary.BigEndian.Uint16(vh)
						c.q1Lock.Lock()
						if _, present := c.q1Pubacks[pID]; present {
							delete(c.q1Pubacks, pID)
						} else {
							c.errs <- fmt.Errorf("error: received unknown PUBACK with ID %d", pID)
						}
						c.q1Lock.Unlock()
					}

					payload = payload[:0]
					if remainLen == 0 {
						if controlAndFlags&0xF0 == broker.PUBLISH {
							if err := c.handlePub(controlAndFlags, vh, payload); err != nil {
								c.errs <- err
								return
							}
						}
						rxState = 0
					} else {
						rxState = 4
					}
				}
				i += toRead
			case 4: // payload
				avail := l - i
				toRead := remainLen
				if avail < toRead {
					toRead = avail
				}
				payload = append(payload, rx[i:i+toRead]...)
				remainLen -= toRead

				if remainLen == 0 {
					switch controlAndFlags & 0xF0 {
					case broker.PUBLISH:
						if err := c.handlePub(controlAndFlags, vh, payload); err != nil {
							c.errs <- err
							return
						}
					case broker.SUBACK:
						c.subbed <- subackInfo{binary.BigEndian.Uint16(vh), payload[0]}
					}
					rxState = 0
				}
				i += toRead
			}
		}
	}
}

func (c *fakeClient) handlePub(flags uint8, vh, payload []byte) error {
	topicLen := binary.BigEndian.Uint16(vh)
	topic := string(vh[2 : 2+topicLen])
	qos := (flags & 0x06) >> 1
	var pID uint16
	if qos == 1 {
		//c.q1PubReadId++
		pID = binary.BigEndian.Uint16(vh[2+topicLen:])
		/*if c.q1PubReadId != pID {
			return fmt.Errorf("publish id %d received, expected %d", pID, c.q1PubReadId)
		}*/
	}

	msg := make([]byte, len(payload))
	copy(msg, payload)
	c.pubs <- pubInfo{topic, flags&0x08 > 0, qos, pID, msg}
	return nil
}

func doConnect(clientId byte, cleanSes bool, expectSp uint8) (*fakeClient, error) {
	conn, err := net.Dial("tcp", "localhost:1883")
	if err != nil {
		return nil, err
	}

	cl := fakeClient{
		errs:      make(chan error),
		conn:      conn,
		pubs:      make(chan pubInfo, 1024),
		connected: make(chan connackInfo),
		subbed:    make(chan subackInfo),
		q1Pubacks: make(map[uint16]struct{}),
	}
	go cl.reader()

	var cf byte
	if cleanSes {
		cf |= 0x02
	}

	_, err = conn.Write([]byte{broker.CONNECT, 13, 0, 4, 'M', 'Q', 'T', 'T', 4, cf, 0, 0, 0, 1, clientId})
	if err != nil {
		return nil, err
	}

	/*connack := []byte{broker.CONNACK, 2, expectSp, 0}
	rx := make([]byte, len(connack))

	nRx, err := conn.Read(rx)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(rx[:nRx], connack) {
		return nil, fmt.Errorf("error connecting to server. Is: %v, must be: %v", rx[:nRx], connack)
	}*/

	select {
	case cInfo := <-cl.connected:
		if cInfo.sp != expectSp {
			return nil, fmt.Errorf("error connecting to server. SessionP: %v, must be: %v", cInfo.sp, expectSp)
		}
		if cInfo.code != 0 {
			return nil, fmt.Errorf("error connecting to server. Return code: %d, must be: %d", cInfo.code, 0)
		}
		//case <-time.After(time.Second):
		//	return nil, fmt.Errorf("error connecting to server. Timeout waiting for CONNACK")
	}

	return &cl, nil
}

func (c *fakeClient) pubMsg(msg []byte, topic string, qos uint8) error {
	c.q1Lock.Lock()
	c.pubId++
	pID := c.pubId
	c.q1Pubacks[pID] = struct{}{}
	c.q1Lock.Unlock()

	pacLen := uint8(2 + len(topic) + len(msg))
	if qos > 0 {
		pacLen += 2
	}

	b := []byte{broker.PUBLISH, pacLen, 0, uint8(len(topic))}
	b = append(b, []byte(topic)...)
	if qos > 0 {
		b = append(b, uint8(pID>>8), uint8(pID))
		b[0] |= 1 << qos
	}

	_, err := c.conn.Write(b)
	if err != nil {
		return err
	}
	_, err = c.conn.Write(msg)
	if err != nil {
		return err
	}

	/*puback := []byte{broker.PUBACK, 2, uint8(c.pubId >> 8), uint8(c.pubId)}
	rx := make([]byte, len(puback))

	nRx, err := c.conn.Read(rx)
	if err != nil {
		return err
	}

	if !bytes.Equal(rx[:nRx], puback) {
		return fmt.Errorf("puback error: %v", rx[:nRx])
	}*/

	return nil
}

func (c *fakeClient) sub(topic string, qos uint8) error {
	var pId uint16 = 5
	b := []byte{broker.SUBSCRIBE | 2, 5 + uint8(len(topic)), uint8(pId >> 8), uint8(pId), 0, uint8(len(topic))}
	b = append(b, []byte(topic)...)
	b = append(b, qos)
	_, err := c.conn.Write(b)
	if err != nil {
		return err
	}

	select {
	case sInfo := <-c.subbed:
		if sInfo.pID != pId {
			return fmt.Errorf("suback error. pID is: %v Must be: %v", sInfo.pID, pId)
		}
		if sInfo.qos != qos {
			return fmt.Errorf("suback error. QoS is: %v Must be: %v", sInfo.qos, 1)
		}
		//case <-time.After(time.Second):
		//	return fmt.Errorf("suback timeout")
	}

	/*suBack := []byte{broker.SUBACK, 3, uint8(pId >> 8), uint8(pId), 1}
	rx := make([]byte, len(suBack))

	nRx, err := c.conn.Read(rx)
	if err != nil {
		return err
	}

	if !bytes.Equal(rx[:nRx], suBack) {
		return fmt.Errorf("suback error. Is: %v Must be: %v", rx[:nRx], suBack)
	}*/

	return nil
}

/*func (c *fakeClient) readQ1(dup, sendPuback bool) error {
	c.q1PubReadId++
	pub := []byte{broker.PUBLISH | 2, 8, 0, 1, 't', uint8(c.q1PubReadId >> 8), uint8(c.q1PubReadId), 'M', 'S', 'G'}
	if dup {
		pub[0] |= 0x08
	}

	rx := make([]byte, len(pub))
	nRx, err := c.conn.Read(rx)
	if err != nil {
		return err
	}

	if !bytes.Equal(rx[:nRx], pub) {
		return fmt.Errorf("publish rx error, is: %v, must be %v", rx[:nRx], pub)
	}

	if sendPuback {
		_, err = c.conn.Write([]byte{broker.PUBACK, 2, uint8(c.q1PubReadId >> 8), uint8(c.q1PubReadId)})
		if err != nil {
			return err
		}
	}

	return nil
}*/

func (c *fakeClient) sendPuback(pID uint16) error {
	_, err := c.conn.Write([]byte{broker.PUBACK, 2, uint8(pID >> 8), uint8(pID)})
	return err
}

func (c *fakeClient) sendDisconnect() error {
	_, err := c.conn.Write([]byte{broker.DISCONNECT, 0})
	return err
}
