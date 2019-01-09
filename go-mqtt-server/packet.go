package go_mqtt_server

// Control Packets
const (
	CONNECT     = 1
	CONNACK     = 2
	PUBLISH     = 3
	PUBACK      = 4
	PUBREC      = 5
	PUBREL      = 6
	PUBCOMP     = 7
	SUBSCRIBE   = 8
	SUBACK      = 9
	UNSUBSCRIBE = 10
	UNSUBACK    = 11
	PINGREQ     = 12
	PINGRESP    = 13
	DISCONNECT  = 14
)

const (
	// Fixed header
	controlAndFlags = iota
	length

	variableHeader
	payload
)

type packet struct {
	controlType     uint8
	flags           uint8
	remainingLength uint32 // max 268,435,455 (256 MB)
	lenMul          uint32

	// Variable header
	vhLen          uint32 // connect: countdown from 10, publish:
	gotVhLen       bool   // publish packet
	variableHeader []byte // publish packet
	identifier     uint16 // publish with QoS>0, etc.

	payload []byte
}
