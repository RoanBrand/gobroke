package tests_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/RoanBrand/gobroke"
	"github.com/sirupsen/logrus"
)

func BenchmarkQoS0(b *testing.B) {
	benchmarkPubs(b, 0)
}

func BenchmarkQoS1(b *testing.B) {
	benchmarkPubs(b, 1)
}

func BenchmarkQoS2(b *testing.B) {
	benchmarkPubs(b, 2)
}

func benchmarkPubs(b *testing.B, qos uint8) {
	logrus.SetLevel(logrus.ErrorLevel)

	errs := make(chan error, 1)
	s := gobroke.Server{}
	defer s.Stop()

	go func() {
		if err := s.Run(context.Background()); err != nil {
			errs <- err
		}
	}()
	time.Sleep(time.Millisecond)

	c1, err := dial("", true, 0, errs)
	if err != nil {
		b.Fatal(err)
	}

	topic := "t"
	err = c1.sub(topic, qos)
	if err != nil {
		b.Fatal(err)
	}

	c2, err := dial("", true, 0, errs)
	if err != nil {
		b.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		for i := 0; i < b.N; i++ {
			pub := <-c1.pubs
			if qos == 1 {
				if err := c1.sendPuback(pub.pID); err != nil {
					errs <- err
					return
				}
			} else if qos == 2 {
				if err := c1.sendPubrec(pub.pID); err != nil {
					errs <- err
					return
				}
			}
		}
		done <- struct{}{}
	}()
	if qos == 2 {
		go func() {
			for i := 0; i < b.N; i++ {
				pubrel := <-c1.pubrels
				if err = c1.sendPubcomp(pubrel); err != nil {
					errs <- err
					return
				}
			}
			done <- struct{}{}
		}()
	}

	msg := make([]byte, 1)
	f1, f2 := 1, 1
	start := make(chan struct{})

	go func() {
		<-start
		for i := 0; i < b.N; i++ {
			if err := c2.pubMsg(msg, topic, qos, func(complete bool, pID uint16) {
				switch qos {
				case 1:
					if !complete { // not puback
						errs <- fmt.Errorf("expect puback, got something else pID %d", pID)
						return
					}

					if f1 == b.N {
						done <- struct{}{}
					} else {
						f1++
					}
				case 2:
					if complete { // PUBCOMPS
						if f2 == b.N && f1 == b.N {
							done <- struct{}{}
						} else {
							f2++
						}
					} else { // PUBRECs
						if err := c2.sendPubrel(pID); err != nil {
							errs <- err
							return
						}
						if f1 < b.N {
							f1++
						}
					}
				}
			}); err != nil {
				errs <- err
				return
			}
		}
		done <- struct{}{}
	}()

	time.Sleep(time.Millisecond * 10) // give chance for everything to startup
	b.ResetTimer()
	close(start)

	select {
	case err := <-errs:
		b.Fatal(err)
	case <-done:
	}
	select {
	case err := <-errs:
		b.Fatal(err)
	case <-done:
	}
	if qos > 0 {
		select {
		case err := <-errs:
			b.Fatal(err)
		case <-done:
		}
	}
	if qos == 2 {
		select {
		case err := <-errs:
			b.Fatal(err)
		case <-done:
		}
	}

	b.StopTimer()
	b.ReportAllocs()

	c1.stop(true)
	c2.stop(true)
}
