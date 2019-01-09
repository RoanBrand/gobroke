package main

import (
	"github.com/RoanBrand/gobroke/go-mqtt-server"
	"log"
)

func main() {
	s := go_mqtt_server.NewServer()
	log.Fatal(s.Start())
}
