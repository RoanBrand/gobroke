package tests_test

import (
	"testing"
	"time"

	"github.com/RoanBrand/gobroke/broker"
	"github.com/sirupsen/logrus"
)

func TestRejoin(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	s, err := broker.NewServer("../../config.json")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()

	errs := make(chan error, 1)
	go func() {
		if err := s.Start(); err != nil {
			errs <- err
		}
	}()

	var oldC1, oldC2 *fakeClient
	var sp1, sp2 uint8
	var c1PubReadId uint16 = 1
	gotPubID := false

	for i := 0; i < 128; i++ {
		c1, err := dial('1', false, sp1, errs)
		if err != nil {
			t.Fatal(err)
		}

		if oldC1 != nil { // prevent reaching max connections on system
			oldC1.conn.Close()
			oldC1 = nil
		}

		sp1 = 1
		err = c1.sub("t", 1)
		if err != nil {
			t.Fatal(err)
		}

		c2, err := dial('2', false, sp2, errs)
		if err != nil {
			t.Fatal(err)
		}
		sp2 = 1

		if oldC2 != nil {
			oldC2.conn.Close()
			oldC2 = nil
		}

		err = c2.pubMsg([]byte("MSG"), "t", 1, nil)
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
		} else {
			oldC1 = c1
		}

		c1, err = dial('1', false, sp1, errs)
		if err != nil {
			t.Fatal(err)
		}
		if oldC1 != nil {
			oldC1.conn.Close()
			oldC1 = nil
		}

		err = c2.pubMsg([]byte("MSG"), "t", 1, nil)
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

		select {
		case err := <-errs:
			t.Fatal(err)
		case pub := <-c1.pubs:
			t.Fatal("unknown pub received:", pub, string(pub.msg))
		case pub := <-c2.pubs:
			t.Fatal("unknown pub received:", pub, string(pub.msg))
		case <-time.After(time.Microsecond * 10):
		}

		oldC1, oldC2 = c1, c2
	}

	// session present must be 0 if cleansession is set.
	_, err = dial('1', true, 0, errs)
	if err != nil {
		t.Fatal(err)
	}
	_, err = dial('2', true, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
}
