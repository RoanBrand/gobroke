package tests_test

import (
	"errors"
	"testing"
	"time"

	"github.com/RoanBrand/gobroke/internal/model"
	"github.com/google/uuid"
)

func testSubscribe(t *testing.T) {
	t.Parallel()
	errs := make(chan error, 1)

	protoErr := errors.New("Bits 3,2,1 and 0 of the fixed header of the SUBSCRIBE Control Packet are reserved and MUST be set to 0,0,1 and 0 respectively. The Server MUST treat any other value as malformed and close the Network Connection [MQTT-3.8.1-1].")

	c, err := dial("", false, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	topic := uuid.NewString()
	var pId uint16 = 10

	b := []byte{model.SUBSCRIBE, 5 + uint8(len(topic)), uint8(pId >> 8), uint8(pId), 0, uint8(len(topic))}
	b = append(b, []byte(topic)...)
	b = append(b, 1)
	err = c.writePacket(b)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 100):
		t.Fatal(protoErr)
	case <-c.subbed:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	c, err = dial("", false, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	b = []byte{model.SUBSCRIBE | 0b1111, 5 + uint8(len(topic)), uint8(pId >> 8), uint8(pId), 0, uint8(len(topic))}
	b = append(b, []byte(topic)...)
	b = append(b, 1)
	err = c.writePacket(b)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 100):
		t.Fatal(protoErr)
	case <-c.subbed:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	c, err = dial("", false, 0, errs)
	if err != nil {
		t.Fatal(err)
	}

	b = []byte{model.SUBSCRIBE | 0b0010, 5 + uint8(len(topic)), uint8(pId >> 8), uint8(pId), 0, uint8(len(topic))}
	b = append(b, []byte(topic)...)
	b = append(b, 1)
	err = c.writePacket(b)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 100):
		t.Fatal(protoErr)
	case <-c.dead:
		t.Fatal(protoErr)
	case suback := <-c.subbed:
		if suback.pID != pId {
			t.Fatal(suback.pID, pId)
		}
		if suback.qos != 1 {
			t.Fatal(suback.qos)
		}
	}
}
