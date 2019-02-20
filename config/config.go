package config

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
)

type Config struct {
	TCP network `json:"tcp"`

	TLS struct {
		network
		Cert string `json:"cert"`
		Key  string `json:"key"`
	} `json:"tls"`

	WS struct {
		network
		CheckOrigin bool `json:"check_origin"`
	} `json:"ws"`

	Log struct {
		File  string `json:"file"`
		Level string `json:"level"`
	} `json:"log"`
}

type network struct {
	Enabled bool   `json:"enabled"`
	Address string `json:"address"`
}

func New(fPath string) (*Config, error) {
	f, err := os.Open(fPath)
	if err != nil {
		return nil, errors.New("error opening config file: " + err.Error())
	}

	defer f.Close()
	c := Config{}
	dec := json.NewDecoder(f)

	err = dec.Decode(&c)
	if err != nil {
		return nil, errors.New("error reading config file: " + err.Error())
	}

	return &c, c.validate()
}

func (c *Config) validate() error {
	if !c.TCP.Enabled && !c.TLS.Enabled && !c.WS.Enabled {
		return errors.New("at least one connection (TCP, TLS or Websocket) needs to be setup")
	}

	if c.TCP.Enabled {
		if !strings.Contains(c.TCP.Address, ":") {
			c.TCP.Address += ":1883" // if just ip/host or nothing specified
		}
	}

	if c.TLS.Enabled {
		if c.TLS.Cert == "" || c.TLS.Key == "" {
			return errors.New("invalid TLS certificate and/or private key file path setup")
		}

		if !strings.Contains(c.TLS.Address, ":") {
			c.TLS.Address += ":8883"
		}
	}

	if c.WS.Enabled {
		if !strings.Contains(c.WS.Address, ":") {
			c.WS.Address += ":80"
		}
	}

	return nil
}
