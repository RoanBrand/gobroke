package main

import (
	"github.com/RoanBrand/gobroke/broker"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(log.DebugLevel)
	s, err := broker.NewServer("config.json")
	if err != nil {
		log.Fatal(err)
	}

	log.Fatal(s.Start())
}
