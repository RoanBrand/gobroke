# gobroke
My own implementation of MQTT v3.1.1 server. Advanced and free to use.

## Features
##### QoS 1
##### Configuration File
* JSON config file for setup. See `config.json`
##### TLS
* Example self-signed certificates included that can be used by MQTT clients on `localhost`
* Make sure MQTT client does not use regular TCP port (default `1883`)

## Build & Run
* Run `go build` inside parent `gobroke` dir
* For now the config file must be in the binary's working dir

## TODO
* OS Service
* Topic Path system & Wildcards
* QoS 2
* Websocket
* Persistence to survive restart
* $SYS Topic
* Rate limiting
* User Auth system with:
    * Client whitelist & blacklist options
    * Client topic subscription whitelist & blacklist options
    * Client topic publish rights whitelist & blacklist
