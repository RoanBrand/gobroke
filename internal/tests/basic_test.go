package tests_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
)

func testRejoin(t *testing.T) {
	t.Parallel()
	errs := make(chan error, 1)

	var oldC1, oldC2 *fakeClient
	c1Name, c2Name := generateNewClientID(), generateNewClientID()
	var sp1, sp2 uint8
	var c1PubReadId uint16 = 1
	gotPubID := false
	topic := uuid.NewString()

	type pubMsg struct {
		topic string
		msg   []byte
		pId   uint16
		qos   uint8
		dup   bool
	}

	pubRx := make(chan pubMsg, 1)

	rxHandler := func(pTopic, pMsg []byte, pID uint16, qos uint8, dup bool) {
		pubRx <- pubMsg{string(topic), pMsg, pID, qos, dup}
	}

	errRx := func(pTopic, pMsg []byte, pID uint16, qos uint8, dup bool) {
		errs <- fmt.Errorf("topic: %s msg: %s, pID: %d, qos: %d, dup %v", pTopic, pMsg, pID, qos, dup)
	}

	for i := 0; i < 10; i++ {
		c1, err := dial(c1Name, false, sp1, errs)
		if err != nil {
			t.Fatal(err)
		}
		c1.pubReceiver = rxHandler

		if oldC1 != nil { // prevent reaching max connections on system
			oldC1.stop(false)
			oldC1 = nil
		}

		sp1 = 1
		err = c1.sub(topic, 1)
		if err != nil {
			t.Fatal(err)
		}

		c2, err := dial(c2Name, false, sp2, errs)
		if err != nil {
			t.Fatal(err)
		}
		c2.pubReceiver = errRx
		sp2 = 1

		if oldC2 != nil {
			oldC2.stop(false)
			oldC2 = nil
		}

		err = c2.pubMsg([]byte("MSG"), topic, 1)
		if err != nil {
			t.Fatal(err)
		}

		pub := <-pubRx
		if !gotPubID {
			c1PubReadId = pub.pId
			gotPubID = true
		}
		if pub.topic != topic || pub.dup || pub.qos != 1 || pub.pId != c1PubReadId || string(pub.msg) != "MSG" {
			t.Fatal("got pub:", pub, "pID must be:", c1PubReadId, "")
		}

		if err := c1.sendPuback(pub.pId); err != nil {
			t.Fatal(err)
		}
		c1PubReadId++

		if i%2 == 0 {
			c1.stop(false)
		} else {
			oldC1 = c1
		}

		c1, err = dial(c1Name, false, sp1, errs)
		if err != nil {
			t.Fatal(err)
		}
		c1.pubReceiver = rxHandler
		if oldC1 != nil {
			oldC1.stop(false)
			oldC1 = nil
		}

		err = c2.pubMsg([]byte("MSG"), topic, 1)
		if err != nil {
			t.Fatal(err)
		}

		pub = <-pubRx
		if pub.topic != topic || pub.dup || pub.qos != 1 || pub.pId != c1PubReadId || string(pub.msg) != "MSG" {
			t.Fatal("got pub:", pub, "pID must be:", c1PubReadId, "")
		}

		if err := c1.sendPuback(pub.pId); err != nil {
			t.Fatal(err)
		}
		c1PubReadId++

		select {
		case err := <-errs:
			t.Fatal(err)
		case pub := <-pubRx:
			t.Fatal("unknown pub received:", pub, string(pub.msg))
		case <-time.After(time.Microsecond * 10):
		}

		oldC1, oldC2 = c1, c2
	}

	// session present must be 0 if cleansession is set.
	_, err := dial(c1Name, true, 0, errs)
	if err != nil {
		t.Fatal(err)
	}
	_, err = dial(c2Name, true, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
}
