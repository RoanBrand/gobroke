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
	c.txCallBack = func(pID uint16, complete bool) {
		errs <- protoErr
	}

	if err = c.pubMsgRaw([]byte("msg"), uuid.NewString(), 0, 1, true); err != nil {
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
	c.txCallBack = func(pID uint16, complete bool) {
		errs <- protoErr
	}

	if err = c.pubMsgRaw([]byte("msg"), uuid.NewString(), 3, 1, false); err != nil {
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

	c.txCallBack = func(pID uint16, complete bool) {
		errs <- protoErr
	}

	if err = c.pubMsg([]byte("msg"), "a/+/c", 1); err != nil {
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

	c.txCallBack = func(pID uint16, complete bool) {
		errs <- protoErr
	}

	if err = c.pubMsg([]byte("msg"), "a/b/#", 1); err != nil {
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

	i := 0
	c1.pubReceiver = func(pTopic, msg []byte, pID uint16, qos uint8, dup bool) {
		if qos != 0 || string(pTopic) != topic {
			errs <- fmt.Errorf("topic: %s msg: %s, pID: %d, qos: %d, dup %v", pTopic, msg, pID, qos, dup)
			return
		}
		k := binary.BigEndian.Uint32(msg)
		if k != uint32(i) {
			errs <- fmt.Errorf("topic: %s msg: %s, pID: %d, qos: %d, dup %v", pTopic, msg, pID, qos, dup)
			return
		}
		i++
		if i == count {
			close(done)
		}
	}

	msg := make([]byte, 4)
	for i := 0; i < count; i++ {
		binary.BigEndian.PutUint32(msg, uint32(i))
		if err := c2.pubMsg(msg, topic, 0); err != nil {
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

	iRx := 1
	c1.pubReceiver = func(pTopic, pMsg []byte, pID uint16, qos uint8, dup bool) {
		if iRx > count {
			errs <- fmt.Errorf("iRx overrun %d. pId: %d", iRx, pID)
			return
		}

		if qos != 1 || string(pTopic) != topic || !bytes.Equal(pMsg, msg) || dup || pID != uint16(iRx) {
			errs <- fmt.Errorf("topic: %s msg: %s, pID: %d, qos: %d, dup %v", pTopic, msg, pID, qos, dup)
			return
		}

		if iRx%2 == 0 {
			if err := c1.sendPuback(uint16(iRx)); err != nil {
				errs <- err
			}
		}

		if iRx == count {
			done <- struct{}{}
		}
		iRx++
	}

	iTx := 1
	c2.txCallBack = func(pID uint16, complete bool) {
		if iTx > count {
			errs <- fmt.Errorf("iTx overrun %d. pId: %d", iTx, pID)
			return
		}
		if !complete { // not puback
			errs <- fmt.Errorf("expect puback, got something else pID %d", pID)
			return
		}

		if pID != uint16(iTx) {
			errs <- fmt.Errorf("expecting puback for pID %d, got for %d", iTx, pID)
			return
		}

		if iTx == count {
			done <- struct{}{}
		}
		iTx++
	}

	for i := 1; i <= count; i++ {
		if err := c2.pubMsg(msg, topic, 1); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case <-done:
		break
	case err = <-errs:
		t.Fatal(err)
	}
	select {
	case <-done:
		break
	case err = <-errs:
		t.Fatal(err)
	}

	if err = c1.stop(true); err != nil {
		t.Fatal(err)
	}

	/*c1, err = dial(c1.ClientID, false, 1, errs)
	if err != nil {
		t.Fatal(err)
	}*/
	c1 = newClient(c1.ClientID, errs)

	iRx = 1
	c1.pubReceiver = func(pTopic, pMsg []byte, pID uint16, qos uint8, dup bool) {
		if iRx > count {
			errs <- fmt.Errorf("iRx overrun %d. pId: %d", iRx, pID)
			return
		}
		if qos != 1 || string(pTopic) != topic || !bytes.Equal(pMsg, msg) || !dup || pID != uint16(iRx) {
			errs <- fmt.Errorf("topic: %s msg: %s, pID: %d, qos: %d, dup %v", pTopic, msg, pID, qos, dup)
			return
		}

		if err := c1.sendPuback(uint16(iRx)); err != nil {
			errs <- err
		}

		if iRx == count-1 {
			done <- struct{}{}
		}
		iRx += 2
	}

	if err := c1.connect(false, 1); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
		break
	case err = <-errs:
		t.Fatal(err)
	}

	if err = c1.stop(true); err != nil {
		t.Fatal(err)
	}

	/*c1, err = dial(c1.ClientID, false, 1, errs)
	if err != nil {
		t.Fatal(err)
	}*/
	oldH := c1.pubReceiver
	c1 = newClient(c1.ClientID, errs)
	c1.pubReceiver = oldH
	if err := c1.connect(false, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal(done)
	case err = <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond):
	}
}

func testQoS2(t *testing.T) {
	t.Parallel()
	errs := make(chan error, 1)

	cRx := newClient("", errs)
	err := cRx.connect(false, 0)
	if err != nil {
		t.Fatal(err)
	}

	topic := uuid.NewString()
	err = cRx.sub(topic, 2)
	if err != nil {
		t.Fatal(err)
	}

	cTx := newClient("", errs)
	err = cTx.connect(false, 0)
	if err != nil {
		t.Fatal(err)
	}

	count := 256
	done := make(chan struct{})
	msg := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	// cRx RX all, send even PUBRECs only.
	iRx := 1
	cRx.pubReceiver = func(pTopic, pMsg []byte, pID uint16, qos uint8, dup bool) {
		if iRx > count {
			errs <- fmt.Errorf("iRx overrun %d. pId: %d", iRx, pID)
			return
		}

		if qos != 2 || string(pTopic) != topic || !bytes.Equal(pMsg, msg) || dup || int(pID) != iRx {
			errs <- fmt.Errorf("topic: %s msg: %s, pID: %d, qos: %d, dup %v", pTopic, msg, pID, qos, dup)
			return
		}
		if iRx%2 == 0 {
			cRx.qL.Lock()
			cRx.q2PubRelTx[pID] = struct{}{}
			cRx.qL.Unlock()
			if err := cRx.sendPubrec(uint16(iRx)); err != nil {
				errs <- err
			}
		}

		if iRx == count {
			done <- struct{}{}
		}
		iRx++
	}

	// cRx expect only even PUBRELs.
	go func() {
		for i := 2; i <= count; i += 2 {
			select {
			case pubrel := <-cRx.pubrels:
				if uint16(i) != pubrel {
					errs <- errors.New(fmt.Sprint("got", pubrel, "expected", i))
					return
				}
				cRx.qL.Lock()
				if _, ok := cRx.q2PubRelTx[pubrel]; ok {
					delete(cRx.q2PubRelTx, pubrel)
				} else {
					cRx.qL.Unlock()
					errs <- errors.New(fmt.Sprint("error: received unknown PUBREL with ID", pubrel))
					return
				}
				cRx.qL.Unlock()
			}
		}
		done <- struct{}{}
	}()

	// cTx Pub all, and only complete even.
	f1, f2 := 1, 2
	cTx.txCallBack = func(pID uint16, complete bool) {
		if f1 > count || f2 > count {
			errs <- fmt.Errorf("f1 or f2 overrun %d %d. pId: %d", f1, f2, pID)
			return
		}

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
				if err := cTx.sendPubrel(pID); err != nil {
					errs <- err
					return
				}
			}
			if f1 < count {
				f1++
			}
		}
	}

	for i := 1; i <= count; i++ {
		if err := cTx.pubMsg(msg, topic, 2); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-done:
	}
	select {
	case err := <-errs:
		t.Fatal(err)
	case <-done:
	}
	select {
	case err := <-errs:
		t.Fatal(err)
	case <-done:
	}

	// cTx Re-pub odd with DUP, and expect their PUBRECs.
	f1 = 1
	cTx.txCallBack = func(pID uint16, complete bool) {
		if f1 > count {
			errs <- fmt.Errorf("f1 overrun %d. pId: %d", f1, pID)
			return
		}
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

	}

	for i := 1; i <= count; i += 2 {
		if err := cTx.pubMsgRaw(msg, topic, 2, uint16(i), true); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-done:
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case pubrel := <-cRx.pubrels:
		t.Fatal(pubrel)
	case <-time.After(time.Millisecond):
	}

	// New cRx & cTx
	if err = cRx.stop(true); err != nil {
		t.Fatal(err)
	}
	if err = cTx.stop(true); err != nil {
		t.Fatal(err)
	}

	// cRx RX duplicate of odd, and send PUBRECs.
	iRx = 1
	cRx.pubReceiver = func(pTopic, pMsg []byte, pID uint16, qos uint8, dup bool) {
		if iRx > count {
			errs <- fmt.Errorf("iRx overrun %d. pId: %d", iRx, pID)
			return
		}

		if qos != 2 || string(pTopic) != topic || !bytes.Equal(pMsg, msg) || !dup || int(pID) != iRx {
			errs <- fmt.Errorf("topic: %s msg: %s, pID: %d, qos: %d, dup %v", pTopic, msg, pID, qos, dup)
			return
		}
		cRx.qL.Lock()
		cRx.q2PubRelTx[pID] = struct{}{}
		cRx.qL.Unlock()
		if err := cRx.sendPubrec(pID); err != nil {
			t.Fatal(err)
		}

		if iRx == count-1 {
			done <- struct{}{}
		}
		iRx += 2
	}

	err = cRx.connect(false, 1)
	if err != nil {
		t.Fatal(err)
	}

	// cRx re-expect only even PUBRELs.
	for i := 2; i <= count; i += 2 {
		select {
		case err := <-errs:
			t.Fatal(err)
		case pubrel := <-cRx.pubrels:
			if uint16(i) != pubrel {
				t.Fatal("must be", i, "is", pubrel)
			}
			if err = cRx.sendPubcomp(pubrel); err != nil {
				t.Fatal(err)
			}
		}
	}

	// cRx PUBRELs for odd.
	go func() {
		for i := 1; i <= count; i += 2 {
			select {
			case pubrel := <-cRx.pubrels:
				if uint16(i) != pubrel {
					errs <- errors.New(fmt.Sprint(pubrel))
					return
				}
			}
		}
		<-done
	}()

	err = cTx.connect(false, 1)
	if err != nil {
		t.Fatal(err)
	}

	// cTx Re-pub odd with DUP again, and complete.
	f1, f2 = 1, 1
	cTx.txCallBack = func(pID uint16, complete bool) {
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
			if err := cTx.sendPubrel(pID); err != nil {
				errs <- err
				return
			}
			if f1 < count-1 {
				f1 += 2
			}
		}

	}

	for i := 1; i <= count; i += 2 {
		if err := cTx.pubMsgRaw(msg, topic, 2, uint16(i), true); err != nil {
			t.Fatal(err)
		}
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-done:
	}

	select {
	case err = <-errs:
		t.Fatal(err)
	case <-done:
	}

	// New cRx
	if err = cRx.stop(true); err != nil {
		t.Fatal(err)
	}
	err = cRx.connect(false, 1)
	if err != nil {
		t.Fatal(err)
	}

	// cRx re-expect only PUBRELs for odd.
	for i := 1; i <= count; i += 2 {
		select {
		case err := <-errs:
			t.Fatal(err)
		case pubrel := <-cRx.pubrels:
			if uint16(i) != pubrel {
				t.Fatal(pubrel)
			}
			if err = cRx.sendPubcomp(pubrel); err != nil {
				t.Fatal(err)
			}
		}
	}

	select {
	case err = <-errs:
		t.Fatal(err)
	case pubrel := <-cRx.pubrels:
		t.Fatal(pubrel)
	case <-time.After(time.Millisecond):
	}

	// New cRx
	if err = cRx.stop(true); err != nil {
		t.Fatal(err)
	}
	err = cRx.connect(false, 1)
	if err != nil {
		t.Fatal(err)
	}

	// Expect nothing.
	select {
	case err = <-errs:
		t.Fatal(err)
	case pubrel := <-cRx.pubrels:
		t.Fatal(pubrel)
	case <-time.After(time.Millisecond):
	}
}
