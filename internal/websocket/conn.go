package websocket

import (
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var dispatch func(net.Conn)

func SetDispatcher(d func(net.Conn)) {
	dispatch = d
}

func Setup(address string, checkOrigin bool, errs chan error) error {
	go func() {
		errs <- http.ListenAndServe(address, http.HandlerFunc(handler(checkOrigin)))
	}()
	return nil
}

func SetupTLS(address, certFile, keyFile string, checkOrigin bool, errs chan error) error {
	go func() {
		errs <- http.ListenAndServeTLS(address, certFile, keyFile, http.HandlerFunc(handler(checkOrigin)))
	}()
	return nil
}

func handler(checkOrigin bool) func(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{
		Subprotocols: []string{"mqtt"}, // [MQTT-6.0.0-4]
	}
	if !checkOrigin {
		up.CheckOrigin = func(*http.Request) bool { return true }
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if protos := websocket.Subprotocols(r); len(protos) == 0 || protos[0] != "mqtt" { // [MQTT-6.0.0-3]
			errMsg := "websocket client not supported. sub protocol must be 'mqtt'"
			http.Error(w, errMsg, http.StatusNotAcceptable)
			return
		}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			errMsg := "unsuccessful websocket negotiation: " + err.Error()
			http.Error(w, errMsg, http.StatusInternalServerError)
			return
		}

		go dispatch(&wsConn{Conn: conn})
	}
}

type wsConn struct {
	*websocket.Conn
	r io.Reader
}

func (c *wsConn) Write(p []byte) (int, error) {
	err := c.WriteMessage(websocket.BinaryMessage, p)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

func (c *wsConn) Read(p []byte) (int, error) {
	for {
		if c.r == nil {
			var err error
			var mt int
			if mt, c.r, err = c.NextReader(); err != nil {
				return 0, err
			}
			if mt != websocket.BinaryMessage { // [MQTT-6.0.0-1]
				return 0, errors.New("not binary message")
			}
		}
		n, err := c.r.Read(p)
		if err == io.EOF {
			c.r = nil
			if n > 0 {
				return n, nil
			} else {
				continue
			}
		}
		return n, err
	}
}

func (c *wsConn) SetDeadline(t time.Time) error {
	if err := c.SetWriteDeadline(t); err != nil {
		return err
	}
	return c.SetReadDeadline(t)
}
