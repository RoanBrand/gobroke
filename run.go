package main

import (
	"log"

	"github.com/RoanBrand/gobroke/broker"
)

func main() {
	s := broker.NewServer()
	log.Fatal(s.Start())
}
