package broker

// Control Packets
const (
	CONNECT     = 1 << 4
	CONNACK     = 2 << 4
	PUBLISH     = 3 << 4
	PUBACK      = 4 << 4
	PUBREC      = 5 << 4
	PUBREL      = 6 << 4
	PUBCOMP     = 7 << 4
	SUBSCRIBE   = 8 << 4
	SUBACK      = 9 << 4
	UNSUBSCRIBE = 10 << 4
	UNSUBACK    = 11 << 4
	PINGREQ     = 12 << 4
	PINGRESP    = 13 << 4
	DISCONNECT  = 14 << 4
)

var pingRespPacket = []byte{PINGRESP, 0}

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
