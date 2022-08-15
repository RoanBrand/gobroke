package model

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

	PUBRELSend = PUBREL | 2
)

// Reason Codes
const (
	NormalDisconnection             = 0
	DisconnectWithWill              = 4
	UnspecifiedError                = 128
	MalformedPacket                 = 129
	ProtocolError                   = 130
	UnsupportedProtocolVersion      = 132
	ServerShuttingDown              = 139
	KeepAliveTimeout                = 141
	SessionTakenOver                = 142
	PayloadFormatInvalid            = 153
	SharedSubscriptionsNotSupported = 158
)

// Properties
const (
	PayloadFormatIndicator       = 1
	MessageExpiryInterval        = 2
	ContentType                  = 3
	ResponseTopic                = 8
	CorrelationData              = 9
	SubscriptionIdentifier       = 11
	SessionExpiryInterval        = 17
	AssignedClientIdentifier     = 18
	AuthenticationMethod         = 21
	AuthenticationData           = 22
	RequestProblemInformation    = 23
	RequestResponseInformation   = 25
	ServerReference              = 28
	ReasonString                 = 31
	ReceiveMaximum               = 33
	TopicAliasMaximum            = 34
	TopicAlias                   = 35
	UserProperty                 = 38
	MaximumPacketSize            = 39
	SharedSubscriptionsAvailable = 42
)

func VariableLengthEncode(packet []byte, l int) []byte {
	for {
		eb := l % 128
		l /= 128
		if l > 0 {
			eb |= 128
		}
		packet = append(packet, byte(eb))
		if l <= 0 {
			break
		}
	}
	return packet
}

func VariableLengthEncodeNoAlloc(l int, f func(eb byte) error) error {
	for {
		eb := l % 128
		l /= 128
		if l > 0 {
			eb |= 128
		}
		if err := f(byte(eb)); err != nil { // No new memory allocation
			return err
		}
		if l <= 0 {
			return nil
		}
	}
}

func LengthToNumberOfVariableLengthBytes(l int) int {
	switch {
	case l < 128:
		return 1
	case l < 16384:
		return 2
	case l < 2097152:
		return 3
	default:
		return 4
	}
}
