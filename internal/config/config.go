package config

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
)

type Config struct {
	// TCP Address optionally specifies the TCP address for the server to listen on,
	// in the form "host:port". If empty, and no other network protocol is used, ":1883" (port 80) is used.
	TCP struct {
		Address string `json:"address"`
	} `json:"tcp"`

	// TLS Address optionally specifies an address for the server to listen on for TLS connections,
	// in the form "host:port". If empty, TLS is not used.
	TLS struct {
		Address string `json:"address"`
		keyPair
	} `json:"tls"`

	// WS Address optionally specifies an address for the server to listen on for Websocket connections,
	// in the form "host:port". If empty, Websocket is not used.
	WS struct {
		Address     string `json:"address"`
		CheckOrigin bool   `json:"check_origin"`
	} `json:"ws"`

	// WSS Address optionally specifies an address for the server to listen on for Secure Websocket connections,
	// in the form "host:port". If empty, Secure Websocket is not used.
	WSS struct {
		Address     string `json:"address"`
		CheckOrigin bool   `json:"check_origin"`
		keyPair
	} `json:"wss"`

	// Log configures optional log output file as well as the log level setting.
	Log struct {
		File  string `json:"file"`
		Level string `json:"level"`
	} `json:"log"`

	// QoS 1&2 unacknowledged message resend timeout in s.
	// Default 60s. Set to -1 to disable timeout based resend.
	// Only for MQTT v3,4 clients. v5 clients never time out.
	// Will always resend unacknowledged messages on new connection,
	// regardless of protocol level and this setting.
	TimeoutQoS12MQTT34 int64 `json:"timeout_qos12_mqtt34"`

	// If specified, and a Client connects with a larger Keep Alive specified,
	// the server will send back this value in CONNACK to client.
	// Only for MQTTv5 clients.
	KeepAliveMaxMQTT5 uint16 `json:"server_keep_alive_max_mqtt5"`
	// If specified, the server will always send back this value to connecting clients,
	// regardless of their specified Keep Alive value.
	// Only for MQTTv5 clients.
	KeepAliveOverrideMQTT5 uint16 `json:"server_keep_alive_override_mqtt5"`
}

type keyPair struct {
	Cert string `json:"cert"`
	Key  string `json:"key"`
}

func (c *Config) LoadFromFile(fPath string) error {
	f, err := os.Open(fPath)
	if err != nil {
		return errors.New("error opening config file: " + err.Error())
	}

	defer f.Close()

	if err = json.NewDecoder(f).Decode(&c); err != nil {
		return errors.New("error reading config file: " + err.Error())
	}

	return c.validate()
}

func (c *Config) validate() error {
	if c.TCP.Address != "" {
		if !strings.Contains(c.TCP.Address, ":") {
			c.TCP.Address += ":1883" // if just ip/host or nothing specified
		}
	}

	if c.TLS.Address != "" {
		if c.TLS.Cert == "" || c.TLS.Key == "" {
			return errors.New("invalid TLS certificate and/or private key file path setup")
		}

		if !strings.Contains(c.TLS.Address, ":") {
			c.TLS.Address += ":8883"
		}
	}

	if c.WS.Address != "" {
		if !strings.Contains(c.WS.Address, ":") {
			c.WS.Address += ":80"
		}
	}

	if c.WSS.Address != "" {
		if c.WSS.Cert == "" || c.WSS.Key == "" {
			return errors.New("invalid TLS certificate and/or private key file path setup for Websocket Secure")
		}

		if !strings.Contains(c.WSS.Address, ":") {
			c.WSS.Address += ":443"
		}
	}

	if c.TimeoutQoS12MQTT34 == 0 {
		c.TimeoutQoS12MQTT34 = 60
	}

	return nil
}
