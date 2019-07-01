package tests_test

import (
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

	errs := make(chan error, 1)
	go func() {
		if err := s.Start(); err != nil {
			errs <- err
		}
	}()

	var sp1, sp2 uint8
	var c1PubReadId uint16 = 1
	gotPubID := false

	for i := 0; i < 500; i++ {
		c1, err := dial('1', false, sp1, errs)
		if err != nil {
			t.Fatal(err)
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
		}

		c1, err = dial('1', false, sp1, errs)
		if err != nil {
			t.Fatal(err)
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
	}

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
