package tests_test

import (
	"testing"
	"time"

	"github.com/RoanBrand/gobroke"
	"github.com/sirupsen/logrus"
)

func TestService(t *testing.T) {
	logrus.SetLevel(logrus.ErrorLevel)
	s := gobroke.Server{}

	t.Cleanup(func() {
		s.Shutdown()
	})

	errs := make(chan error, 1)
	go func() {
		if err := s.Run(); err != nil {
			errs <- err
		}
	}()
	time.Sleep(time.Millisecond)

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
