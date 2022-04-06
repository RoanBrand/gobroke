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

	return nil
}
