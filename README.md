# gobroke
My own implementation of MQTT v3.1.1 server. Advanced and free to use.

## Features
##### Configuration File
* JSON config file for setup. See `config.json`
* Config example and description:
```javascript
{
	"tcp": {
		"enabled": true,
		"address": ":1883"                 // ip/host & port
	},
	"tls": {                                   // Secure TCP
		"enabled": true,
		"address": ":8883",
		"cert": "path_to_TLS_certificate",
		"key": "path_to_private_key"
	},
	"ws": {                                    // Websocket
		"enabled": true,
		"address": ":80",
		"check_origin": false              // check request origin
	},	
	"log": {
		"file": "path_to_log_file",        // log to file if specified
		"level": "info"                    // error, warn, info, debug
	}
}
```
* Most of config is optional, but one of `tcp`, `tls`, or `ws` must be specified and enabled
##### OS Service
* Build then copy the binary and config file to a folder you want and run `gobroke -service install`
* Service can then be started with `gobroke -service start` or from the OS Service Manager
##### QoS 1
##### TLS
* Example self-signed certificates included that can be used by MQTT clients on `localhost`
* Make sure MQTT client does not use regular TCP port (default `8883`, not `1883`)
##### Websocket
* The address must contain host/ip only and no URL. Currently it is served over all URLs

## Run
* `go run run.go -c="config.json"`

## Build
* Run `go build` inside parent `gobroke` dir
* Binary can now be run as is if `config.json` present in the same dir
* Config path can be overridden with `gobroke -c="path_to_config_file"`

## TODO
* QoS 2 Subscriptions (Clients are able to send QoS 2 messages currently though)
* Persistence to survive restart
* $SYS Topic
* Rate limiting
* User Auth system with:
    * Client whitelist & blacklist options
    * Client topic subscription whitelist & blacklist options
    * Client topic publish rights whitelist & blacklist
