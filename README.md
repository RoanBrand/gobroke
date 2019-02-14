# gobroke
My own implementation of MQTT v3.1.1 server. Advanced and free to use.

## Features
##### QoS 1
##### Configuration File
* JSON config file for setup. See `config.json`
* Config Description:
```javascript
{
	"tcp": {
		"enabled": true,
		"address": ":1883"             // ip/host & port
	},
	"tls": {                               // For secure TCP
		"enabled": true,
		"address": ":8883",
		"cert": "path_to_TLS_certificate",
		"key": "path_to_private_key"
	},
	"log": {
	    "file": "path_to_log_file",        // log to file if specified
	    "level": "info"                    // error, warn, info, debug
	}
}
```
##### OS Service
* Build then copy the binary and config file to a folder you want and run `gobroke -service install`
* Service can then be started with `gobroke -service start` or from the OS Service Manager
##### TLS
* Example self-signed certificates included that can be used by MQTT clients on `localhost`
* Make sure MQTT client does not use regular TCP port (default `8883`, not `1883`)

## Run
* `go run run.go -c="config.json"`

## Build
* Run `go build` inside parent `gobroke` dir
* Binary can now be run as is if `config.json` present in the same dir
* Config path can be overridden with `gobroke -c="path_to_config_file"`

## TODO
* QoS 2 Subscriptions (Clients are able to send QoS 2 messages currently though)
* Websocket
* Persistence to survive restart
* $SYS Topic
* Rate limiting
* User Auth system with:
    * Client whitelist & blacklist options
    * Client topic subscription whitelist & blacklist options
    * Client topic publish rights whitelist & blacklist
