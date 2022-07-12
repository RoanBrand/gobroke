package tests_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func testPublish(t *testing.T) {
	t.Parallel()
	errs := make(chan error, 1)

	c, err := dial("", false, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	protoErr := errors.New("The DUP flag MUST be set to 0 for all QoS 0 messages [MQTT-3.3.1-2].")

	if err = c.pubMsgRaw([]byte("msg"), uuid.NewString(), 0, 1, true, func(complete bool, pID uint16) {
		errs <- protoErr
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 100):
		t.Fatal(protoErr)
	case <-c.dead:
	}

	c, err = dial("", false, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	protoErr = errors.New("A PUBLISH Packet MUST NOT have both QoS bits set to 1. If a Server or Client receives a PUBLISH Packet which has both QoS bits set to 1 it MUST close the Network Connection [MQTT-3.3.1-4].")

	if err = c.pubMsgRaw([]byte("msg"), uuid.NewString(), 3, 1, false, func(complete bool, pID uint16) {
		errs <- protoErr
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 100):
		t.Fatal(protoErr)
	case <-c.dead:
	}

	protoErr = errors.New("The Topic Name in the PUBLISH Packet MUST NOT contain wildcard characters [MQTT-3.3.2-2].")

	c, err = dial("", false, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	if err = c.pubMsg([]byte("msg"), "a/+/c", 1, func(complete bool, pID uint16) {
		errs <- protoErr
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 100):
		t.Fatal(protoErr)
	case <-c.dead:
	}

	c, err = dial("", false, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	if err = c.pubMsg([]byte("msg"), "a/b/#", 1, func(complete bool, pID uint16) {
		errs <- protoErr
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 100):
		t.Fatal(protoErr)
	case <-c.dead:
	}

	// "A PUBLISH Packet MUST NOT contain a Packet Identifier if its QoS value is set to 0 [MQTT-2.3.1-5]."
	// Impossible to test because server cannot detect this. If pID included with Qos0, it is read as the start of the payload instead.
}

func testQoS0(t *testing.T) {
	t.Parallel()
	errs := make(chan error, 1)

	c1, err := dial("", true, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	topic := uuid.NewString()
	err = c1.sub(topic, 0)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := dial("", true, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	count := 256
	done := make(chan struct{})
	go func() {
		for i := 0; i < count; i++ {
			pub := <-c1.pubs
			if pub.qos != 0 || pub.topic != topic {
				errs <- fmt.Errorf("%+v", pub)
				return
			}
			k := binary.BigEndian.Uint32(pub.msg)
			if k != uint32(i) {
				errs <- fmt.Errorf("%+v", pub)
				return
			}
		}
		close(done)
	}()

	msg := make([]byte, 4)
	for i := 0; i < count; i++ {
		binary.BigEndian.PutUint32(msg, uint32(i))
		if err := c2.pubMsg(msg, topic, 0, nil); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case err = <-errs:
		t.Fatal(err)
	case <-done:
	}
}

func testQoS1(t *testing.T) {
	t.Parallel()
	errs := make(chan error, 1)

	c1, err := dial("", false, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	topic := uuid.NewString()
	err = c1.sub(topic, 1)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := dial("", false, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	count := 256
	done := make(chan struct{})
	msg := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	f1 := 1
	for i := 1; i <= count; i++ {
		if err := c2.pubMsg(msg, topic, 1, func(complete bool, pID uint16) {
			if !complete { // not puback
				errs <- fmt.Errorf("expect puback, got something else pID %d", pID)
				return
			}

			if pID != uint16(f1) {
				errs <- fmt.Errorf("expecting puback for pID %d, got for %d", f1, pID)
				return
			}

			if f1 == count {
				close(done)
			} else {
				f1++
			}
		}); err != nil {
			t.Fatal(err)
		}
	}

	for i := 1; i <= count; i++ {
		select {
		case err := <-errs:
			t.Fatal(err)
		case pub := <-c1.pubs:
			if pub.qos != 1 || pub.topic != topic || !bytes.Equal(pub.msg, msg) || pub.dup || pub.pID != uint16(i) {
				t.Fatal(pub)
			}

			if i%2 == 0 {
				if err = c1.sendPuback(uint16(i)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	<-done

	select {
	case pub := <-c1.pubs:
		t.Fatal(pub)
	case err = <-errs:
		t.Fatal(err)
	default:
	}

	if err = c1.stop(true); err != nil {
		t.Fatal(err)
	}
	c1, err = dial(c1.ClientID, false, 1, errs)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= count; i += 2 {
		select {
		case err := <-errs:
			t.Fatal(err)
		case pub := <-c1.pubs:
			if pub.qos != 1 || pub.topic != topic || !bytes.Equal(pub.msg, msg) || !pub.dup || pub.pID != uint16(i) {
				t.Fatal(i, pub)
			}

			if err = c1.sendPuback(uint16(i)); err != nil {
				t.Fatal(err)
			}
		}
	}

	select {
	case pub := <-c1.pubs:
		t.Fatal(pub)
	case err = <-errs:
		t.Fatal(err)
	default:
	}

	if err = c1.stop(true); err != nil {
		t.Fatal(err)
	}
	c1, err = dial(c1.ClientID, false, 1, errs)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case pub := <-c1.pubs:
		t.Fatal(pub)
	case err = <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond):
	}
}

func testQoS2(t *testing.T) {
	t.Parallel()
	errs := make(chan error, 1)

	c1 := newClient("", errs)
	err := c1.connect(false, 0)
	if err != nil {
		t.Fatal(err)
	}

	topic := uuid.NewString()
	err = c1.sub(topic, 2)
	if err != nil {
		t.Fatal(err)
	}

	c2 := newClient("", errs)
	err = c2.connect(false, 0)
	if err != nil {
		t.Fatal(err)
	}

	count := 256
	done := make(chan struct{})
	msg := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	// C2 Pub all, and only complete even.
	f1, f2 := 1, 2
	for i := 1; i <= count; i++ {
		if err := c2.pubMsg(msg, topic, 2, func(complete bool, pID uint16) {
			if complete { // PUBCOMPS
				if int(pID) != f2 { // only expect even PUBCOMPs
					errs <- fmt.Errorf("expecting pubcomp for pID %d, got for %d", f2, pID)
					return
				}
				if f2 == count && f1 == count {
					done <- struct{}{}
				} else {
					f2 += 2
				}
			} else { // PUBRECs
				if int(pID) != f1 {
					errs <- fmt.Errorf("expecting pubrec for pID %d, got for %d", f1, pID)
					return
				}
				if pID%2 == 0 { // just send even PUBRELs
					if err := c2.sendPubrel(pID); err != nil {
						errs <- err
						return
					}
				}
				if f1 < count {
					f1++
				}
			}
		}); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-done:
	}

	// C2 Re-pub odd with DUP, and expect their PUBRECs.
	f1 = 1
	for i := 1; i <= count; i += 2 {
		if err := c2.pubMsgRaw(msg, topic, 2, uint16(i), true, func(complete bool, pID uint16) {
			if complete {
				errs <- fmt.Errorf("expecting pubrec for pID %d, got pubcomp", pID)
				return
			}

			if int(pID) != f1 {
				errs <- fmt.Errorf("expecting pubrec for pID %d, got for %d", f1, pID)
				return
			}
			if f1 == count-1 {
				done <- struct{}{}
			} else {
				f1 += 2
			}

		}); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-done:
	}

	// C1 RX all, send even PUBRECs only.
	for i := 1; i <= count; i++ {
		select {
		case err := <-errs:
			t.Fatal(err)
		case pub := <-c1.pubs:
			if pub.qos != 2 || pub.topic != topic || !bytes.Equal(pub.msg, msg) || pub.dup || int(pub.pID) != i {
				t.Fatal(pub)
			}
			if i%2 == 0 {
				c1.qL.Lock()
				c1.q2PubRelTx[pub.pID] = struct{}{}
				c1.qL.Unlock()
				if err = c1.sendPubrec(uint16(i)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	// C1 expect only even PUBRELs.
	for i := 2; i <= count; i += 2 {
		select {
		case err := <-errs:
			t.Fatal(err)
		case pubrel := <-c1.pubrels:
			if uint16(i) != pubrel {
				t.Fatal("got", pubrel, "expected", i)
			}
			c1.qL.Lock()
			if _, ok := c1.q2PubRelTx[pubrel]; ok {
				delete(c1.q2PubRelTx, pubrel)
			} else {
				t.Fatal("error: received unknown PUBREL with ID", pubrel)
			}
			c1.qL.Unlock()
		}

	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case pub := <-c1.pubs:
		t.Fatal(pub)
	case pubrel := <-c1.pubrels:
		t.Fatal(pubrel)
	case <-time.After(time.Millisecond):
	}

	// New C1 & C2
	if err = c1.stop(true); err != nil {
		t.Fatal(err)
	}
	if err = c2.stop(true); err != nil {
		t.Fatal(err)
	}
	err = c1.connect(false, 1)
	if err != nil {
		t.Fatal(err)
	}
	err = c2.connect(false, 1)
	if err != nil {
		t.Fatal(err)
	}

	// C2 Re-pub odd with DUP again, and complete.
	f1, f2 = 1, 1
	for i := 1; i <= count; i += 2 {
		if err := c2.pubMsgRaw(msg, topic, 2, uint16(i), true, func(complete bool, pID uint16) {
			if complete { // PUBCOMPS
				if int(pID) != f2 { // only expect even PUBCOMPs
					errs <- fmt.Errorf("expecting pubcomp for pID %d, got for %d", f2, pID)
					return
				}
				if f2 == count-1 && f1 == count-1 {
					done <- struct{}{}
				} else {
					f2 += 2
				}
			} else { // PUBRECs
				if int(pID) != f1 {
					errs <- fmt.Errorf("expecting pubrec for pID %d, got for %d", f1, pID)
					return
				}
				if err := c2.sendPubrel(pID); err != nil {
					errs <- err
					return
				}
				if f1 < count-1 {
					f1 += 2
				}
			}

		}); err != nil {
			t.Fatal(err)
		}
	}

	// C1 re-expect only even PUBRELs.
	for i := 2; i <= count; i += 2 {
		select {
		case err := <-errs:
			t.Fatal(err)
		case pubrel := <-c1.pubrels:
			if uint16(i) != pubrel {
				t.Fatal("must be", i, "is", pubrel)
			}
			if err = c1.sendPubcomp(pubrel); err != nil {
				t.Fatal(err)
			}
		}
	}

	// C1 RX duplicate of odd, and send PUBRECs.
	for i := 1; i <= count; i += 2 {
		select {
		case err := <-errs:
			t.Fatal(err)
		case pub := <-c1.pubs:
			if pub.qos != 2 || pub.topic != topic || !bytes.Equal(pub.msg, msg) || !pub.dup || int(pub.pID) != i {
				t.Fatal(i, pub)
			}

			c1.qL.Lock()
			c1.q2PubRelTx[pub.pID] = struct{}{}
			c1.qL.Unlock()
			if err = c1.sendPubrec(uint16(i)); err != nil {
				t.Fatal(err)
			}
		}
	}

	// C1 PUBRELs for odd.
	for i := 1; i <= count; i += 2 {
		select {
		case err := <-errs:
			t.Fatal(err)
		case pubrel := <-c1.pubrels:
			if uint16(i) != pubrel {
				t.Fatal(pubrel)
			}
		}
	}

	select {
	case err = <-errs:
		t.Fatal(err)
	case pub := <-c1.pubs:
		t.Fatal(pub)
	case pubrel := <-c1.pubrels:
		t.Fatal(pubrel)
	case <-done:
	}

	// New C1
	if err = c1.stop(true); err != nil {
		t.Fatal(err)
	}
	err = c1.connect(false, 1)
	if err != nil {
		t.Fatal(err)
	}

	// C1 re-expect only PUBRELs for odd.
	for i := 1; i <= count; i += 2 {
		select {
		case err := <-errs:
			t.Fatal(err)
		case pubrel := <-c1.pubrels:
			if uint16(i) != pubrel {
				t.Fatal(pubrel)
			}
			if err = c1.sendPubcomp(pubrel); err != nil {
				t.Fatal(err)
			}
		}
	}

	select {
	case err = <-errs:
		t.Fatal(err)
	case pub := <-c1.pubs:
		t.Fatal(pub)
	case pubrel := <-c1.pubrels:
		t.Fatal(pubrel)
	case <-time.After(time.Millisecond):
	}

	// New C1
	if err = c1.stop(true); err != nil {
		t.Fatal(err)
	}
	err = c1.connect(false, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Expect nothing.
	select {
	case err = <-errs:
		t.Fatal(err)
	case pub := <-c1.pubs:
		t.Fatal(pub)
	case pubrel := <-c1.pubrels:
		t.Fatal(pubrel)
	case <-time.After(time.Millisecond):
	}
}
