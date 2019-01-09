package main

import (
	"github.com/RoanBrand/gobroke/broker"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(log.DebugLevel)
	s := broker.NewServer()

	log.Println("Starting MQTT server on port 1883")
	log.Fatal(s.Start())
}
