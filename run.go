package main

import (
	"flag"
	"os"
	"path/filepath"

	"github.com/RoanBrand/gobroke/internal/broker"
	"github.com/kardianos/service"
	log "github.com/sirupsen/logrus"
)

type program struct {
	server     *broker.Server
	configPath string
	execPath   string
}

func (p *program) Start(s service.Service) error {
	var err error
	if p.server, err = broker.NewServer(p.configPath); err != nil {
		return err
	}

	go func() {
		if err := p.server.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	return nil
}

func (p *program) Stop(s service.Service) error {
	p.server.Stop()
	return nil
}

func main() {
	svcFlag := flag.String("service", "", "Control the system service.")
	cnfFlag := flag.String("c", "", "Path of config file.")
	flag.Parse()

	ePath, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	eDir, _ := filepath.Split(ePath)

	// Set defaults before config override.
	if service.Interactive() {
		log.SetLevel(log.DebugLevel)
	} else {
		f, err := os.OpenFile(filepath.Join(eDir, "gobroke.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal(err)
		}
		log.SetOutput(f)
	}

	var cnfPath string
	if *cnfFlag == "" {
		cnfPath = filepath.Join(eDir, "config.json")
		log.Warn("no config file path arg specified, assuming ", cnfPath)
	} else {
		cnfPath = *cnfFlag
	}

	svcConfig := service.Config{
		Name:        "gobroke",
		DisplayName: "gobroke MQTT server",
		Description: "gobroke MQTT v3.1.1 server. See https://github.com/RoanBrand/gobroke",
	}
	prg := program{configPath: cnfPath}

	s, err := service.New(&prg, &svcConfig)
	if err != nil {
		log.Fatal(err)
	}

	if len(*svcFlag) != 0 {
		err := service.Control(s, *svcFlag)
		if err != nil {
			log.Printf("Valid actions: %q\n", service.ControlAction)
			log.Fatal(err)
		}
		return
	}

	err = s.Run()
	if err != nil {
		log.Fatal(err)
	}
}
