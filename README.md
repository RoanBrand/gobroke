# gobroke
My own implementation of MQTT server. Advanced and free to use.
Supports MQTT v3-5.

## Installation
```bash
go get github.com/RoanBrand/gobroke
```
## Basic Usage
```go
import "github.com/RoanBrand/gobroke"

s := gobroke.Server{}
err := s.Run(context.Background())
if err != nil {
	panic(err)
}
```
* `s.Run()` starts a MQTT broker listening on TCP port 1883.
* It blocks while service is running until `s.Stop()` is called, or provided context is cancelled.
* Will return `nil` if successfully shut down, or provide an error otherwise.

## Extended Configuration
```go
s := gobroke.Server{}
s.TCP.Address = ""
s.TLS.Address = ":8883"
s.TLS.Cert = "certs/example.crt"
s.TLS.Key = "certs/example.key"
s.WS.Address = ":8080"

s.Run(context.Background())
```
* You can configure network protocols TCP, TLS, Websocket, and Secure Websocket (WSS).
* Setting the `Address` field to `""` disables the network protocol and listener.
* If all addresses are blank, or no config is provided, the server will default to using just TCP ":1883"
* See `internal/config/config.go` for all options.

## OS Service (Standalone Build)
* The service can be built into a standalone server binary executable by running `make build`
* Copy the `gobroke` binary and `config.json` to server folder and run `gobroke -service install`
* Service can then be started with `gobroke -service start` or from the OS Service Manager
* The config file can be omitted, in which case the server will just listen on TCP ":1883"

* Example JSON config file for standalone server:
```javascript
{
	"tcp": {
		"address": ":1883"                 // ip/host & port
	},
	"tls": {                                   // Secure TCP
		"address": ":8883",
		"cert": "path_to_TLS_certificate",
		"key": "path_to_private_key"
	},
	"ws": {                                    // Websocket
		"address": ":80",
		"check_origin": false              // check request origin
	},
	"wss": {                                   // Secure Websocket over HTTPS
		"address": ":443",
		"check_origin": false,
		"cert": "path_to_TLS_certificate",
		"key": "path_to_private_key"
	},
	"log": {
		"file": "path_to_log_file",        // log to file if specified
		"level": "info"                    // error, warn, info, debug
	}
}
```
* See `config.json` for an example config file.
* Config file path can be overridden with `gobroke -c="path_to_config_file"`

##### TLS
* Example self-signed certificates included that can be used by MQTT clients on `localhost`
* Make sure MQTT client does not use regular TCP port (default `8883`, not `1883`)
##### Websocket
* The address must contain host/ip only and no URL. Currently it is served over all URLs
* `check_origin` ensures that Origin request header is present and that the origin host is equal to request Host header before accepting a connection

## Test
* `make test`

## TODO
* MQTT 5:
	* Subscription Ids
	* Shared Subscriptions
	* Request/Response
	* Response Information
	* Topic Aliases
	* Receive Maximum
	* Max Packet Size
	* Server Keep Alive
	* AUTH

* Tests to cover entire MQTT spec.
* Persistence (levelDB?)
* $SYS Topic
* User Auth system with:
    * Client whitelist & blacklist options
    * Client topic subscription whitelist & blacklist options
    * Client topic publish rights whitelist & blacklist
* Rate limiting
