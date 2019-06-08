package broker_test

import (
	"github.com/RoanBrand/gobroke/broker"
	"github.com/sirupsen/logrus"
	"testing"
	"time"
)

func TestPersist(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	errPipe := make(chan error)
	s, err := broker.NewServer("config.json")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	go func() {
		if err := s.Start(); err != nil {
			errPipe <- err
		}
	}()

	c1, err := doConnect('1', false, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = c1.sub("topic1", 1)
	if err != nil {
		t.Fatal(err)
	}

	s.Stop()
	s, err = broker.NewServer("config.json")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	go func() {
		if err := s.Start(); err != nil {
			errPipe <- err
		}
	}()

	c2, err := doConnect('2', true, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = c2.pubMsg([]byte("msg1"), "topic1", 1)
	if err != nil {
		t.Fatal(err)
	}

	c1, err = doConnect('1', false, 1)
	if err != nil {
		t.Fatal(err)
	}

	pub := <-c1.pubs
	if pub.qos != 1 || pub.topic != "topic1" || string(pub.msg) != "msg1" || pub.dup {
		t.Fatalf("got pub: %+v\n", pub)
	}
	if err = c1.sendPuback(pub.pID); err != nil {
		t.Fatal(err)
	}

last:
	for {
		select {
		case pub := <-c1.pubs:
			t.Fatal("unknown pub received:", pub, string(pub.msg))
		case <-time.After(time.Microsecond * 200):
			break last
		case err := <-errPipe:
			t.Fatal(err)
		}

	}

	// cleanup
	if err = c1.sendDisconnect(); err != nil {
		t.Fatal(err)
	}
	c1.conn.Close()
	c1, err = doConnect('1', true, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = c1.sendDisconnect(); err != nil {
		t.Fatal(err)
	}
	c1.conn.Close()
	time.Sleep(time.Millisecond)

	select {
	case err := <-errPipe:
		t.Fatal(err)
	default:
	}
}
