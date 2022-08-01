package tests_test

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
)

func testRejoin2(t *testing.T) {
	t.Parallel()
	errs := make(chan error, 1)
	rand.Seed(time.Now().UnixNano())

	cID := uuid.NewString()
	var expectOldSession uint8 = 0

	for i := 0; i < 50; i++ {
		dialCleanSession := rand.Intn(2) == 0
		if dialCleanSession {
			expectOldSession = 0
		}

		_, err := dial(cID, dialCleanSession, expectOldSession, errs)
		if err != nil {
			t.Fatal(err)
		}

		if dialCleanSession {
			expectOldSession = 0
		} else {
			expectOldSession = 1
		}
	}
}

func testConnect(t *testing.T) {
	t.Parallel()

	errs := make(chan error, 1)
	c := newClient("", errs)
	// t.Log("first packet from server must be CONNECT", c.ClientID)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	protoErr := errors.New("After a Network Connection is established by a Client to a Server, the first Packet sent from the Client to the Server MUST be a CONNECT Packet [MQTT-3.1.0-1].")
	c.txCallBack = func(pID uint16, complete bool) {
		errs <- protoErr
	}

	if err := c.pubMsg([]byte("not a CONNECT first"), "topic", 0); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal("server did not close connection:", protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	c = newClient("", errs)
	// t.Log("second CONNECT must result in disconnect", c.ClientID)
	if err := c.connect(false, 0); err != nil {
		t.Fatal(err)
	}

	if err := c.sendConnectPacket(false); err != nil {
		t.Fatal(err)
	}

	protoErr = errors.New("The Server MUST process a second CONNECT Packet sent from a Client as a protocol violation and disconnect the Client [MQTT-3.1.0-2]")

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	c = newClient("", errs)
	// t.Log("protocol name must be valid", c.ClientID)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	// The MQTT v3.1.1 spec does not specify if CONNACK with code 1 must or may be sent in this case, but we do, so test for it.
	protoErr = errors.New("If the protocol name is incorrect the Server MAY disconnect the Client, or it MAY continue processing the CONNECT packet in accordance with some other specification. In the latter case, the Server MUST NOT continue to process the CONNECT packet in line with this specification [MQTT-3.1.2-1]")

	if err := c.sendUnknownProtocolNameConnectPacket(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.dead:
		t.Fatal("no connack received:", protoErr)
	case connack := <-c.connacks:
		if connack.code != 1 {
			t.Fatal(protoErr)
		}
		if connack.sp != 0 {
			t.Fatal("unexpected session present")
		}
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	protoErr = errors.New("The Server MUST respond to the CONNECT Packet with a CONNACK return code 0x01 (unacceptable protocol level) and then disconnect the Client if the Protocol Level is not supported by the Server [MQTT-3.1.2-2]")

	c = newClient("", errs)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l := byte(len(c.ClientID))
	connectPack := []byte{1 << 4, 12 + l, 0, 4, 'M', 'Q', 'T', 'T', 2, 0, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.dead:
		t.Fatal("no connack received:", protoErr)
	case connack := <-c.connacks:
		if connack.code != 1 {
			t.Fatal(protoErr, connack.code)
		}
		if connack.sp != 0 {
			t.Fatal("unexpected session present")
		}
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	protoErr = errors.New("The Server MUST validate that the reserved flag in the CONNECT Control Packet is set to zero and disconnect the Client if it is not zero [MQTT-3.1.2-3].")

	c = newClient("", errs)
	// t.Log("CONNECT reserved flag must be 0", c.ClientID)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	connectPack = []byte{1 << 4, 12 + l, 0, 4, 'M', 'Q', 'T', 'T', 4, 1, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	// Will Flag & Message
	protoErr = errors.New("If the Will Flag is set to 1, the Will QoS and Will Retain fields in the Connect Flags will be used by the Server, and the Will Topic and Will Message fields MUST be present in the payload [MQTT-3.1.2-9].")

	// topic & message not present in payload with will flag = 1
	c = newClient("", errs)
	// t.Log("Will Flag 1: topic & message not present in payload", c.ClientID)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	connectPack = []byte{1 << 4, 12 + l, 0, 4, 'M', 'Q', 'T', 'T', 4, 4, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	// message not present in payload with will flag = 1
	c = newClient("", errs)
	// t.Log("Will Flag 1: message not present in payload", c.ClientID)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	willTopic := []byte("will topic")
	wtl := byte(len(willTopic))
	connectPack = []byte{1 << 4, 12 + l + 2 + wtl, 0, 4, 'M', 'Q', 'T', 'T', 4, 4, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	connectPack = append(connectPack, 0, wtl)
	connectPack = append(connectPack, willTopic...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	c = newClient("", errs)
	// t.Log("Will Flag 1: topic & message present in payload", c.ClientID)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	willMsg := []byte("will message")
	wml := byte(len(willMsg))
	connectPack = []byte{1 << 4, 12 + l + 2 + wtl + 2 + wml, 0, 4, 'M', 'Q', 'T', 'T', 4, 4, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	connectPack = append(connectPack, 0, wtl)
	connectPack = append(connectPack, willTopic...)
	connectPack = append(connectPack, 0, wml)
	connectPack = append(connectPack, willMsg...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-c.dead:
		t.Fatal(protoErr)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case connackInfo := <-c.connacks:
		if connackInfo.code != 0 {
			t.Fatal(protoErr)
		}
	}

	protoErr = errors.New("If the Will Flag is set to 0 the Will QoS and Will Retain fields in the Connect Flags MUST be set to zero and the Will Topic and Will Message fields MUST NOT be present in the payload [MQTT-3.1.2-11].")

	// will qos = 1 with will flag = 0
	c = newClient("", errs)
	// t.Log("Will Flag 0: Will QoS 1", c.ClientID)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	connectPack = []byte{1 << 4, 12 + l, 0, 4, 'M', 'Q', 'T', 'T', 4, 8, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		// If the Will Flag is set to 0, then the Will QoS MUST be set to 0 (0x00) [MQTT-3.1.2-13].
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	// will qos = 2 with will flag = 0
	c = newClient("", errs)
	// t.Log("Will Flag 0: Will QoS 2", c.ClientID)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	connectPack = []byte{1 << 4, 12 + l, 0, 4, 'M', 'Q', 'T', 'T', 4, 16, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		// If the Will Flag is set to 0, then the Will QoS MUST be set to 0 (0x00) [MQTT-3.1.2-13].
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	// will retain = 1 with will flag = 0
	c = newClient("", errs)
	// t.Log("Will Flag 0: Will Retain 1", c.ClientID)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	connectPack = []byte{1 << 4, 12 + l, 0, 4, 'M', 'Q', 'T', 'T', 4, 32, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr) // If the Will Flag is set to 0, then the Will Retain Flag MUST be set to 0 [MQTT-3.1.2-15].
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	// will topic or message included in payload with will flag = 0
	c = newClient("", errs)
	// t.Log("Will Flag 0: Will Topic or Message included", c.ClientID)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	connectPack = []byte{1 << 4, 12 + l + 2 + wtl, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	connectPack = append(connectPack, 0, wtl)
	connectPack = append(connectPack, willTopic...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	protoErr = errors.New("If the Will Flag is set to 1, the value of Will QoS can be 0 (0x00), 1 (0x01), or 2 (0x02). It MUST NOT be 3 (0x03) [MQTT-3.1.2-14].")

	// will qos = 3 with will flag = 1
	c = newClient("", errs)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	connectPack = []byte{1 << 4, 12 + l, 0, 4, 'M', 'Q', 'T', 'T', 4, 28, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	// User Name

	protoErr = errors.New("If the User Name Flag is set to 0, a user name MUST NOT be present in the payload [MQTT-3.1.2-18].")

	// User Name Flag = 0, with user name in payload
	c = newClient("", errs)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	userName := []byte("user name")
	unl := byte(len(userName))
	connectPack = []byte{1 << 4, 12 + l + 2 + unl, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	connectPack = append(connectPack, 0, unl)
	connectPack = append(connectPack, userName...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	protoErr = errors.New("If the User Name Flag is set to 1, a user name MUST be present in the payload [MQTT-3.1.2-19].")

	// User Name Flag = 1, with NO user name in payload
	c = newClient("", errs)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	connectPack = []byte{1 << 4, 12 + l, 0, 4, 'M', 'Q', 'T', 'T', 4, 128, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	// User Name Flag = 1, with a user name in payload
	c = newClient("", errs)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	connectPack = []byte{1 << 4, 12 + l + 2 + unl, 0, 4, 'M', 'Q', 'T', 'T', 4, 128, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	connectPack = append(connectPack, 0, unl)
	connectPack = append(connectPack, userName...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-c.dead:
		t.Fatal(protoErr)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case connackInfo := <-c.connacks:
		if connackInfo.code != 0 {
			t.Fatal(protoErr)
		}
	}

	// Password

	protoErr = errors.New("If the Password Flag is set to 0, a password MUST NOT be present in the payload [MQTT-3.1.2-20].")

	// Password Flag = 0, with password in payload
	c = newClient("", errs)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	password := []byte("pass word")
	pl := byte(len(password))
	connectPack = []byte{1 << 4, 12 + l + 2 + pl, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	connectPack = append(connectPack, 0, pl)
	connectPack = append(connectPack, password...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	protoErr = errors.New("If the Password Flag is set to 1, a password MUST be present in the payload [MQTT-3.1.2-21].")

	// User Name = 1, Password Flag = 1, with NO password in payload
	c = newClient("", errs)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	connectPack = []byte{1 << 4, 12 + l + 2 + unl, 0, 4, 'M', 'Q', 'T', 'T', 4, 192, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	connectPack = append(connectPack, 0, unl)
	connectPack = append(connectPack, userName...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	// User Name = 1, Pasword Flag = 1, with a password in payload
	c = newClient("", errs)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	connectPack = []byte{1 << 4, 12 + l + 2 + unl + 2 + pl, 0, 4, 'M', 'Q', 'T', 'T', 4, 192, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	connectPack = append(connectPack, 0, unl)
	connectPack = append(connectPack, userName...)
	connectPack = append(connectPack, 0, pl)
	connectPack = append(connectPack, password...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-c.dead:
		t.Fatal(protoErr)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case connackInfo := <-c.connacks:
		if connackInfo.code != 0 {
			t.Fatal(protoErr)
		}
	}

	protoErr = errors.New("If the User Name Flag is set to 0, the Password Flag MUST be set to 0 [MQTT-3.1.2-22].")

	// User Name = 0, Pasword Flag = 1, with a password in payload
	c = newClient("", errs)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	l = byte(len(c.ClientID))
	connectPack = []byte{1 << 4, 12 + l + 2 + pl, 0, 4, 'M', 'Q', 'T', 'T', 4, 64, 0, 0, 0, l}
	connectPack = append(connectPack, []byte(c.ClientID)...)
	connectPack = append(connectPack, 0, pl)
	connectPack = append(connectPack, password...)

	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	// Client Identifier

	protoErr = errors.New("The Client Identifier (ClientId) MUST be present and MUST be the first field in the CONNECT packet payload [MQTT-3.1.3-3].")

	// connect with no ClientId
	c = newClient("", errs)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	connectPack = []byte{1 << 4, 10, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0}
	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}

	// A Server MAY allow a Client to supply a ClientId that has a length of zero bytes,
	// however if it does so the Server MUST treat this as a special case and assign a unique ClientId to that Client.
	// It MUST then process the CONNECT packet as if the Client had provided that unique ClientId [MQTT-3.1.3-6].

	protoErr = errors.New("If the Client supplies a zero-byte ClientId, the Client MUST also set CleanSession to 1 [MQTT-3.1.3-7].")

	// zero ClientId, with CleanSession 1
	c = newClient("", errs)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	connectPack = []byte{1 << 4, 12, 0, 4, 'M', 'Q', 'T', 'T', 4, 2, 0, 0, 0, 0}
	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-c.dead:
		t.Fatal(protoErr)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case connackInfo := <-c.connacks:
		if connackInfo.code != 0 {
			t.Fatal(protoErr)
		}
	}

	protoErr = errors.New("If the Client supplies a zero-byte ClientId with CleanSession set to 0, the Server MUST respond to the CONNECT Packet with a CONNACK return code 0x02 (Identifier rejected) and then close the Network Connection [MQTT-3.1.3-8].")

	// zero ClientId, with CleanSession 0
	c = newClient("", errs)
	if err := c.dialOnly(); err != nil {
		t.Fatal(err)
	}

	connectPack = []byte{1 << 4, 12, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, 0, 0}
	if err := c.writePacket(connectPack); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.dead:
		t.Fatal("no connack received:", protoErr)
	case connack := <-c.connacks:
		if connack.code != 2 {
			t.Fatal(protoErr)
		}
		if connack.sp != 0 {
			t.Fatal("unexpected session present")
		}
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Millisecond * 500):
		t.Fatal(protoErr)
	case <-c.connacks:
		t.Fatal(protoErr)
	case <-c.dead:
	}
}
