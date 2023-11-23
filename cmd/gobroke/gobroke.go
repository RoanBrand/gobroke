package main

import (
	"flag"
	"os"
	"path/filepath"

	"github.com/RoanBrand/gobroke"
	"github.com/kardianos/service"
	log "github.com/sirupsen/logrus"
)

type program struct {
	server     gobroke.Server
	configFlag string
	execDir    string
}

/*type auther struct{}

func (a *auther) AuthUser(clientId string, username, password []byte) error {
	if string(username) == "roan" && string(password) == "brand" {
		return nil
	}

	return errors.New("not whitelisted")
}

func (a *auther) AuthPublish(clientId string, topicName []byte) error {
	return nil
}

func (a *auther) AuthSubscription(clientId string, topicFilter []byte) error {
	if clientId != "goodClient" && string(topicFilter) == "restricted" {
		return errors.New("restricted")
	}
	return nil
}*/

func (p *program) Start(s service.Service) error {
	if p.configFlag != "" {
		if err := p.server.LoadFromFile(p.configFlag); err != nil {
			return err
		}
		log.Infoln("Using config file:", p.configFlag)
	} else {
		toTry := filepath.Join(p.execDir, "config.json")
		if fileExists(toTry) {
			if err := p.server.LoadFromFile(toTry); err != nil {
				return err
			}
			log.Infoln("Using config file:", toTry)
		} else {
			log.Infoln("No config file specified or found. Using defaults.")
		}
	}

	go func() {
		/*a := auth.NewBasicAuth()
		a.ToggleGuestAccess(true)
		a.RegisterUser("roan", "roan", "brand")
		a.AllowSubscription("testtopic/#", "roan")
		a.AllowPublish("testtopic", "bird")

		p.server.Auther = a*/
		if err := p.server.Run(); err != nil {
			log.Fatal(err)
		}
	}()
	return nil
}

func (p *program) Stop(s service.Service) error {
	p.server.Shutdown()
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

	prg := program{configFlag: *cnfFlag, execDir: eDir}
	svcConfig := service.Config{
		Name:        "gobroke",
		DisplayName: "gobroke MQTT server",
		Description: "gobroke MQTT server. See https://github.com/RoanBrand/gobroke",
	}

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

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
