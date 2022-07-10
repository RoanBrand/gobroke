package tests_test

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoanBrand/gobroke/internal/model"
	"github.com/google/uuid"
)

type fakeClient struct {
	ClientID string

	errs          chan error
	conn          net.Conn
	pubs          chan pubInfo
	pubrels       chan uint16
	connacks      chan connackInfo
	subbed        chan subackInfo
	pingResponses chan struct{}

	qL        sync.Mutex
	q1Pubacks map[uint16]pubctrl // pubs to server

	q2Pubrecs  map[uint16]pubctrl
	q2Pubcomps map[uint16]pubctrl
	q2PubRelTx map[uint16]struct{}

	acks, workBuf []byte
	pIDs          chan uint16 // for publish and subscribe packets

	tx      *bufio.Writer
	txFlush chan struct{}
	txLock  sync.Mutex

	wg     sync.WaitGroup
	toKill int32
	dead   chan struct{}
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
			close(c.dead)
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
					case model.PUBLISH:
						vhLen = 0
					case model.PINGRESP:
						c.pingResponses <- struct{}{}
					default: // CONNACK, PUBACK, SUBACK
						vhLen = 2
					}

					if remainLen == 0 {
						rxState = 0
					} else {
						vh = vh[:0]
						if controlAndFlags&0xF0 == model.PUBLISH {
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
					case model.CONNACK:
						c.connacks <- connackInfo{vh[0], vh[1]}
					case model.PUBACK:
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
					case model.PUBREC:
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
					case model.PUBCOMP:
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

					case model.PUBREL:
						pID := binary.BigEndian.Uint16(vh)
						c.pubrels <- pID
					}

					payload = payload[:0]
					if remainLen == 0 {
						if controlAndFlags&0xF0 == model.PUBLISH {
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
					case model.PUBLISH:
						if err := c.handlePub(controlAndFlags, vh, payload); err != nil {
							c.errs <- err
							return
						}
					case model.SUBACK:
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

func dial(clientId string, cleanSes bool, expectSp uint8, errs chan error) (*fakeClient, error) {
	c := newClient(clientId, errs)
	return c, c.connect(cleanSes, expectSp)
}

func newClient(clientId string, errs chan error) *fakeClient {
	if len(clientId) > 64 {
		errs <- fmt.Errorf("clientId too long (%d)", len(clientId))
		return nil
	}

	c := fakeClient{
		ClientID:      clientId,
		errs:          errs,
		pubs:          make(chan pubInfo, 1024),
		pubrels:       make(chan uint16, 1024),
		connacks:      make(chan connackInfo),
		subbed:        make(chan subackInfo),
		pingResponses: make(chan struct{}),
		q1Pubacks:     make(map[uint16]pubctrl),
		q2Pubrecs:     make(map[uint16]pubctrl),
		q2Pubcomps:    make(map[uint16]pubctrl),
		q2PubRelTx:    make(map[uint16]struct{}),

		acks:    make([]byte, 4),
		workBuf: make([]byte, 0, 1024),
		pIDs:    make(chan uint16, 65536),
		tx:      &bufio.Writer{},
		txFlush: make(chan struct{}, 1),
	}
	for i := 1; i <= 65535; i++ {
		c.pIDs <- uint16(i)
	}

	if len(clientId) == 0 {
		c.ClientID = generateNewClientID()
	}

	return &c
}

func generateNewClientID() string {
	uuid := [16]byte(uuid.New())
	return hex.EncodeToString(uuid[:])
}

func (c *fakeClient) dialOnly() error {
	var err error
	c.conn, err = net.DialTimeout("tcp", "localhost:1883", time.Second)
	if err != nil {
		return err
	}

	atomic.StoreInt32(&c.toKill, 0)
	c.dead = make(chan struct{})
	c.tx.Reset(c.conn)
	c.wg.Add(2)
	go c.startWriter()
	go c.reader()

	return nil
}

func (c *fakeClient) sendConnectPacket(cleanSession bool) error {
	var cf byte
	if cleanSession {
		cf |= 0x02
	}

	l := byte(len(c.ClientID))
	connectPack := []byte{model.CONNECT, 12 + l, 0, 4, 'M', 'Q', 'T', 'T', 4, cf, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	return c.writePacket(connectPack)
}

func (c *fakeClient) sendUnknownProtocolNameConnectPacket() error {
	l := byte(len(c.ClientID))
	connectPack := []byte{model.CONNECT, 12 + l, 0, 4, 'F', 'A', 'K', 'E', 4, 0, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	return c.writePacket(connectPack)
}

func (c *fakeClient) connect(cleanSes bool, expectSp uint8) error {
	if err := c.dialOnly(); err != nil {
		return err
	}

	if err := c.sendConnectPacket(cleanSes); err != nil {
		return err
	}

	select {
	case cInfo := <-c.connacks:
		if cInfo.sp != expectSp {
			return fmt.Errorf("error connecting to server. SessionP: %v, must be: %v", cInfo.sp, expectSp)
		}
		if cInfo.code != 0 {
			return fmt.Errorf("error connecting to server. Return code: %d, must be: %d", cInfo.code, 0)
		}
	case <-c.dead:
		return errors.New("client stopped. server most likely closed connection")
	case <-time.After(time.Second):
		return fmt.Errorf("timed out waiting for CONNACK")
	}

	return nil
}

func (c *fakeClient) stop(sendDisconnect bool) error {
	if sendDisconnect {
		if err := c.sendDisconnect(); err != nil {
			return err
		}
	}

	atomic.StoreInt32(&c.toKill, 1)
	if len(c.txFlush) == 0 {
		select {
		case c.txFlush <- struct{}{}:
		default:
		}
	}

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
	pacLen := 2 + len(topic) + len(msg)
	if qos > 0 {
		pacLen += 2
	}

	c.workBuf = c.workBuf[:0]
	c.workBuf = append(c.workBuf, model.PUBLISH)
	c.workBuf = model.VariableLengthEncode(c.workBuf, pacLen)
	c.workBuf = append(c.workBuf, uint8(len(topic)>>8), uint8(len(topic)))
	c.workBuf = append(c.workBuf, []byte(topic)...)
	if qos > 0 {
		c.workBuf = append(c.workBuf, uint8(pubID>>8), uint8(pubID))
		if qos == 3 {
			c.workBuf[0] |= 0b00000110
		} else {
			c.workBuf[0] |= 1 << qos
		}

	}
	if dup {
		c.workBuf[0] |= 0x08
	}

	c.workBuf = append(c.workBuf, msg...)

	err := c.writePacket(c.workBuf)
	return err
}

func (c *fakeClient) sub(topic string, qos uint8) error {
	pId := <-c.pIDs
	b := []byte{model.SUBSCRIBE | 2}
	b = model.VariableLengthEncode(b, len(topic)+5)
	b = append(b, uint8(pId>>8), uint8(pId), uint8(len(topic)>>8), uint8(len(topic)))
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
		c.pIDs <- pId
	case <-c.dead:
		return errors.New("client stopped. server most likely closed connection")
	case <-time.After(time.Second):
		return fmt.Errorf("timed out waiting for SUBACK")
	}

	return nil
}

func (c *fakeClient) sendPuback(pID uint16) error {
	c.workBuf = c.workBuf[:0]
	c.workBuf = append(c.workBuf, model.PUBACK, 2, uint8(pID>>8), uint8(pID))
	//c.acks[0], c.acks[1], c.acks[2], c.acks[3] = model.PUBACK, 2, uint8(pID >> 8), uint8(pID)
	err := c.writePacket(c.workBuf)
	return err
}

func (c *fakeClient) sendPubrec(pID uint16) error {
	c.workBuf = c.workBuf[:0]
	c.workBuf = append(c.workBuf, model.PUBREC, 2, uint8(pID>>8), uint8(pID))
	//c.acks[0], c.acks[1], c.acks[2], c.acks[3] = model.PUBREC, 2, uint8(pID >> 8), uint8(pID)
	err := c.writePacket(c.workBuf)
	return err
}

func (c *fakeClient) sendPubrel(pID uint16) error {
	c.acks[0], c.acks[1], c.acks[2], c.acks[3] = model.PUBREL|2, 2, uint8(pID>>8), uint8(pID)
	err := c.writePacket(c.acks)
	return err
}

func (c *fakeClient) sendPubcomp(pID uint16) error {
	c.acks[0], c.acks[1], c.acks[2], c.acks[3] = model.PUBCOMP, 2, uint8(pID>>8), uint8(pID)
	err := c.writePacket(c.acks)
	return err
}

func (c *fakeClient) sendPing() error {
	c.acks[0], c.acks[1] = model.PINGREQ, 0
	return c.writePacket(c.acks[:2])
}

func (c *fakeClient) sendDisconnect() error {
	c.acks[0], c.acks[1] = model.DISCONNECT, 0
	err := c.writePacket(c.acks[:2])
	return err
}

func (c *fakeClient) writePacket(p []byte) error {
	c.txLock.Lock()
	if _, err := c.tx.Write(p); err != nil {
		c.txLock.Unlock()
		return err
	}
	c.txLock.Unlock()

	if len(c.txFlush) == 0 {
		select {
		case c.txFlush <- struct{}{}:
		default:
		}
	}
	return nil
}

func (c *fakeClient) startWriter() {
	defer c.wg.Done()
	for range c.txFlush {
		c.txLock.Lock()

		if c.tx.Buffered() > 0 {
			if err := c.tx.Flush(); err != nil {
				c.txLock.Unlock()
				if strings.Contains(err.Error(), "use of closed") {
					return
				}
				c.errs <- err
				return
			}
		}
		if atomic.LoadInt32(&c.toKill) == 1 {
			c.txLock.Unlock()
			if err := c.conn.Close(); err != nil {
				c.errs <- err
			}
			return
		}

		c.txLock.Unlock()
	}
}
