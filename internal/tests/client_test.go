package tests_test

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoanBrand/gobroke/broker"
)

type fakeClient struct {
	errs      chan error
	conn      net.Conn
	pubs      chan pubInfo
	pubrels   chan uint16
	connected chan connackInfo
	subbed    chan subackInfo

	qL        sync.Mutex
	q1Pubacks map[uint16]pubctrl // pubs to server

	q2Pubrecs  map[uint16]pubctrl
	q2Pubcomps map[uint16]pubctrl
	q2PubRelTx map[uint16]struct{}

	acks, workBuf []byte
	pIDs          chan uint16

	tx      *bufio.Writer
	txFlush chan struct{}
	txLock  sync.Mutex

	wg   sync.WaitGroup
	dead int32
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

type pubctrl struct {
	callback func(complete bool, pID uint16)
}

func (c *fakeClient) reader() {
	rx := make([]byte, 4096)
	var rxState, controlAndFlags uint8
	var remainLen, lenMul, vhLen uint32
	vh, payload := make([]byte, 0, 512), make([]byte, 0, 512)
	defer c.wg.Done()

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
						c.qL.Lock()
						if pi, ok := c.q1Pubacks[pID]; ok {
							delete(c.q1Pubacks, pID)
							c.qL.Unlock()

							if pi.callback != nil {
								pi.callback(true, pID)
							}
							c.pIDs <- pID
						} else {
							c.qL.Unlock()
							c.errs <- fmt.Errorf("error: received unknown PUBACK with ID %d", pID)
							return
						}
					case broker.PUBREC:
						pID := binary.BigEndian.Uint16(vh)
						c.qL.Lock()
						if pi, ok := c.q2Pubrecs[pID]; ok {
							delete(c.q2Pubrecs, pID)
							c.q2Pubcomps[pID] = pi
							c.qL.Unlock()

							if pi.callback != nil {
								pi.callback(false, pID)
							} else {
								if err = c.sendPubrel(pID); err != nil {
									c.errs <- err
									return
								}
							}
						} else {
							c.qL.Unlock()
							c.errs <- fmt.Errorf("error: received unknown PUBREC with ID %d", pID)
							return
						}
					case broker.PUBCOMP:
						pID := binary.BigEndian.Uint16(vh)
						c.qL.Lock()
						if pi, ok := c.q2Pubcomps[pID]; ok {
							delete(c.q2Pubcomps, pID)
							c.qL.Unlock()

							if pi.callback != nil {
								pi.callback(true, pID)
							}
							c.pIDs <- pID
						} else {
							c.qL.Unlock()
							c.errs <- fmt.Errorf("error: received unknown PUBCOMP with ID %d", pID)
							return
						}

					case broker.PUBREL:
						pID := binary.BigEndian.Uint16(vh)
						c.pubrels <- pID
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
	if qos > 0 {
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

func dial(clientId byte, cleanSes bool, expectSp uint8, errs chan error) (*fakeClient, error) {
	c := newClient(errs)
	return c, c.connect(clientId, cleanSes, expectSp)
}

func newClient(errs chan error) *fakeClient {
	c := fakeClient{
		errs:       errs,
		pubs:       make(chan pubInfo, 1024),
		pubrels:    make(chan uint16, 1024),
		connected:  make(chan connackInfo),
		subbed:     make(chan subackInfo),
		q1Pubacks:  make(map[uint16]pubctrl),
		q2Pubrecs:  make(map[uint16]pubctrl),
		q2Pubcomps: make(map[uint16]pubctrl),
		q2PubRelTx: make(map[uint16]struct{}),

		acks:    make([]byte, 4),
		workBuf: make([]byte, 0, 1024),
		pIDs:    make(chan uint16, 65536),
		tx:      &bufio.Writer{},
		txFlush: make(chan struct{}, 1),
	}
	for i := 1; i <= 65535; i++ {
		c.pIDs <- uint16(i)
	}

	return &c
}

func (c *fakeClient) connect(clientId byte, cleanSes bool, expectSp uint8) error {
	var err error
	c.conn, err = net.DialTimeout("tcp", "localhost:1883", time.Second)
	if err != nil {
		return err
	}

	c.tx.Reset(c.conn)
	c.wg.Add(2)
	atomic.StoreInt32(&c.dead, 0)
	go c.startWriter()
	go c.reader()

	var cf byte
	if cleanSes {
		cf |= 0x02
	}

	err = c.writePacket([]byte{broker.CONNECT, 13, 0, 4, 'M', 'Q', 'T', 'T', 4, cf, 0, 0, 0, 1, clientId})
	if err != nil {
		return err
	}

	select {
	case cInfo := <-c.connected:
		if cInfo.sp != expectSp {
			return fmt.Errorf("error connecting to server. SessionP: %v, must be: %v", cInfo.sp, expectSp)
		}
		if cInfo.code != 0 {
			return fmt.Errorf("error connecting to server. Return code: %d, must be: %d", cInfo.code, 0)
		}
	}

	return nil
}

func (c *fakeClient) stop() error {
	if err := c.sendDisconnect(); err != nil {
		return err
	}
	atomic.StoreInt32(&c.dead, 1)
	if err := c.conn.Close(); err != nil {
		return err
	}

	c.txLock.Lock()
	if len(c.txFlush) == 0 {
		select {
		case c.txFlush <- struct{}{}:
		default:
		}
	}
	c.txLock.Unlock()

	c.wg.Wait()
	return nil
}

func (c *fakeClient) pubMsg(msg []byte, topic string, qos uint8, callback func(complete bool, pID uint16)) error {
	var pID uint16
	if qos > 0 {
		pID = <-c.pIDs
	}
	return c.pubMsgRaw(msg, topic, qos, pID, false, callback)
}

func (c *fakeClient) pubMsgRaw(msg []byte, topic string, qos uint8, pubID uint16, dup bool, callback func(complete bool, pID uint16)) error {
	if qos > 0 {
		c.qL.Lock()
		if qos == 1 {
			c.q1Pubacks[pubID] = pubctrl{callback: callback}
		} else {
			c.q2Pubrecs[pubID] = pubctrl{callback: callback}
		}
		c.qL.Unlock()
	}
	pacLen := uint8(2 + len(topic) + len(msg))
	if qos > 0 {
		pacLen += 2
	}

	c.workBuf = c.workBuf[:0]
	c.workBuf = append(c.workBuf, broker.PUBLISH, pacLen, 0, uint8(len(topic)))
	c.workBuf = append(c.workBuf, []byte(topic)...)
	if qos > 0 {
		c.workBuf = append(c.workBuf, uint8(pubID>>8), uint8(pubID))
		c.workBuf[0] |= 1 << qos
	}
	if dup {
		c.workBuf[0] |= 0x08
	}

	c.workBuf = append(c.workBuf, msg...)

	err := c.writePacket(c.workBuf)
	return err
}

func (c *fakeClient) sub(topic string, qos uint8) error {
	var pId uint16 = 5
	b := []byte{broker.SUBSCRIBE | 2, 5 + uint8(len(topic)), uint8(pId >> 8), uint8(pId), 0, uint8(len(topic))}
	b = append(b, []byte(topic)...)
	b = append(b, qos)
	err := c.writePacket(b)
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

	return nil
}

func (c *fakeClient) sendPuback(pID uint16) error {
	c.workBuf = c.workBuf[:0]
	c.workBuf = append(c.workBuf, broker.PUBACK, 2, uint8(pID>>8), uint8(pID))
	//c.acks[0], c.acks[1], c.acks[2], c.acks[3] = broker.PUBACK, 2, uint8(pID >> 8), uint8(pID)
	err := c.writePacket(c.workBuf)
	return err
}

func (c *fakeClient) sendPubrec(pID uint16) error {
	c.workBuf = c.workBuf[:0]
	c.workBuf = append(c.workBuf, broker.PUBREC, 2, uint8(pID>>8), uint8(pID))
	//c.acks[0], c.acks[1], c.acks[2], c.acks[3] = broker.PUBREC, 2, uint8(pID >> 8), uint8(pID)
	err := c.writePacket(c.workBuf)
	return err
}

func (c *fakeClient) sendPubrel(pID uint16) error {
	c.acks[0], c.acks[1], c.acks[2], c.acks[3] = broker.PUBREL|2, 2, uint8(pID>>8), uint8(pID)
	err := c.writePacket(c.acks)
	return err
}

func (c *fakeClient) sendPubcomp(pID uint16) error {
	c.acks[0], c.acks[1], c.acks[2], c.acks[3] = broker.PUBCOMP, 2, uint8(pID>>8), uint8(pID)
	err := c.writePacket(c.acks)
	return err
}

func (c *fakeClient) sendDisconnect() error {
	c.acks[0], c.acks[1] = broker.DISCONNECT, 0
	err := c.writePacket(c.acks[:2])
	return err
}

func (c *fakeClient) writePacket(p []byte) error {
	c.txLock.Lock()
	if _, err := c.tx.Write(p); err != nil {
		c.txLock.Unlock()
		return err
	}

	if len(c.txFlush) == 0 {
		select {
		case c.txFlush <- struct{}{}:
		default:
		}
	}

	c.txLock.Unlock()
	return nil
}

func (c *fakeClient) startWriter() {
	defer c.wg.Done()
	for range c.txFlush {
		c.txLock.Lock()
		if atomic.LoadInt32(&c.dead) == 1 {
			c.txLock.Unlock()
			return
		}

		if c.tx.Buffered() > 0 {
			if err := c.tx.Flush(); err != nil {
				if strings.Contains(err.Error(), "use of closed") {
					c.txLock.Unlock()
					return
				}
				c.txLock.Unlock()
				c.errs <- err
				return
			}
		}
		c.txLock.Unlock()
	}
}
