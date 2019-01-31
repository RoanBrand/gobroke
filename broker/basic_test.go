package broker_test

import (
	"bytes"
	"fmt"
	"net"
	"testing"

	"github.com/RoanBrand/gobroke/broker"
	"github.com/sirupsen/logrus"
)

func TestRejoin(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	s := broker.NewServer()

	errPipe := make(chan error)
	go func() {
		errPipe <- s.Start()
	}()

	var sp1, sp2 uint8
	var c1PubReadId uint16

	for i := 0; i < 10; i++ {
		c1, err := doConnect('1', sp1)
		if err != nil {
			t.Fatal(err)
		}
		sp1 = 1
		err = c1.subQ1()
		if err != nil {
			t.Fatal(err)
		}

		c2, err := doConnect('2', sp2)
		if err != nil {
			t.Fatal(err)
		}
		sp2 = 1

		err = c2.pubQ1()
		if err != nil {
			t.Fatal(err)
		}
		err = c1.readQ1(&c1PubReadId, false, true)
		if err != nil {
			t.Fatal(err)
		}

		if i%2 == 0 {
			c1.conn.Close()
		}

		c1, err = doConnect('1', sp1)
		if err != nil {
			t.Fatal(err)
		}

		err = c2.pubQ1()
		if err != nil {
			t.Fatal(err)
		}

		err = c1.readQ1(&c1PubReadId, false, true)
		if err != nil {
			t.Fatal(err)
		}
	}

	select {
	case err := <-errPipe:
		t.Fatal(err)
	default:
	}
}

func TestResendPendingQos1AtReconnect(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	s := broker.NewServer()

	errPipe := make(chan error)
	go func() {
		errPipe <- s.Start()
	}()

	var c1PubReadId uint16

	// startup
	c1, err := doConnect('1', 0)
	if err != nil {
		t.Fatal(err)
	}

	err = c1.subQ1()
	if err != nil {
		t.Fatal(err)
	}

	c2, err := doConnect('2', 0)
	if err != nil {
		t.Fatal(err)
	}

	// 1 with puback
	err = c2.pubQ1()
	if err != nil {
		t.Fatal(err)
	}

	err = c1.readQ1(&c1PubReadId, false, true)
	if err != nil {
		t.Fatal(err)
	}

	// 2,3 without puback
	err = c2.pubQ1()
	if err != nil {
		t.Fatal(err)
	}

	err = c1.readQ1(&c1PubReadId, false, false)
	if err != nil {
		t.Fatal(err)
	}

	err = c2.pubQ1()
	if err != nil {
		t.Fatal(err)
	}

	err = c1.readQ1(&c1PubReadId, false, false)
	if err != nil {
		t.Fatal(err)
	}

	c1.conn.Close()

	// 4, 5 no receive
	err = c2.pubQ1()
	if err != nil {
		t.Fatal(err)
	}
	err = c2.pubQ1()
	if err != nil {
		t.Fatal(err)
	}

	c1, err = doConnect('1', 1)
	if err != nil {
		t.Fatal(err)
	}

	// receive 2,3,4,5
	c1PubReadId = 1
	err = c1.readQ1(&c1PubReadId, true, true)
	if err != nil {
		t.Fatal(err)
	}
	err = c1.readQ1(&c1PubReadId, true, true)
	if err != nil {
		t.Fatal(err)
	}
	err = c1.readQ1(&c1PubReadId, false, true)
	if err != nil {
		t.Fatal(err)
	}
	err = c1.readQ1(&c1PubReadId, false, true)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errPipe:
		t.Fatal(err)
	default:
	}
}

type client struct {
	conn  net.Conn
	rx    []byte
	pubId uint16
	//pubReadId uint16
}

func doConnect(clientId byte, expectSp uint8) (*client, error) {
	conn, err := net.Dial("tcp", "localhost:1883")
	if err != nil {
		return nil, err
	}

	//defer conn.Close()

	cl := client{conn: conn, rx: make([]byte, 1024)}

	_, err = conn.Write([]byte{broker.CONNECT, 13, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 5, 0, 1, clientId})
	if err != nil {
		return nil, err
	}

	//conn.SetDeadline(time.Now().Add(time.Second))
	nRx, err := conn.Read(cl.rx)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal(cl.rx[:nRx], []byte{broker.CONNACK, 2, expectSp, 0}) {
		return nil, fmt.Errorf("error connecting to server %v", cl.rx[:nRx])
	}

	return &cl, nil
}

func (c *client) pubQ1() error {
	c.pubId++

	_, err := c.conn.Write([]byte{broker.PUBLISH | 2, 8, 0, 1, 't', uint8(c.pubId >> 8), uint8(c.pubId), 'M', 'S', 'G'})
	if err != nil {
		return err
	}

	nRx, err := c.conn.Read(c.rx)
	if err != nil {
		return err
	}

	if !bytes.Equal(c.rx[:nRx], []byte{broker.PUBACK, 2, uint8(c.pubId >> 8), uint8(c.pubId)}) {
		return fmt.Errorf("puback error: %v", c.rx[:nRx])
	}

	return nil
}

func (c *client) subQ1() error {
	var pId uint16 = 5
	_, err := c.conn.Write([]byte{broker.SUBSCRIBE | 2, 6, uint8(pId >> 8), uint8(pId), 0, 1, 't', 1})
	if err != nil {
		return err
	}

	nRx, err := c.conn.Read(c.rx)
	if err != nil {
		return err
	}

	suBack := []byte{broker.SUBACK, 3, uint8(pId >> 8), uint8(pId), 1}
	if !bytes.Equal(c.rx[:nRx], suBack) {
		return fmt.Errorf("suback error. Is: %v Must be: %v", c.rx[:nRx], suBack)
	}

	return nil
}

func (c *client) readQ1(pubReadId *uint16, dup, sendPuback bool) error {
	*pubReadId++

	nRx, err := c.conn.Read(c.rx)
	if err != nil {
		return err
	}

	pub := []byte{broker.PUBLISH | 2, 8, 0, 1, 't', uint8(*pubReadId >> 8), uint8(*pubReadId), 'M', 'S', 'G'}
	if dup {
		pub[0] |= 0x08
	}
	if !bytes.Equal(c.rx[:nRx], pub) {
		return fmt.Errorf("publish rx error, is: %v, must be %v", c.rx[:nRx], pub)
	}

	if sendPuback {
		_, err = c.conn.Write([]byte{broker.PUBACK, 2, uint8(*pubReadId >> 8), uint8(*pubReadId)})
		if err != nil {
			return err
		}
	}

	return nil
}
