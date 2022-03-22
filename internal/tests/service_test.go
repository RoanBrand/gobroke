package tests_test

import (
	"testing"

	"github.com/RoanBrand/gobroke/broker"

	"github.com/sirupsen/logrus"
)

func TestService(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	s, err := broker.NewServer("../../config.json")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		s.Stop()
	})

	errs := make(chan error, 1)
	go func() {
		if err := s.Start(); err != nil {
			errs <- err
		}
	}()

	t.Run("cleansession and sessionpresent", testRejoin)
	t.Run("new clients", testRejoin2)
	t.Run("qos0", testQoS0)
	t.Run("qos1", testQoS1)
	t.Run("qos2", testQoS2)
	t.Run("connect", testConnect)
	t.Run("publish", testPublish)
	t.Run("subscribe", testSubscribe)

	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
}
