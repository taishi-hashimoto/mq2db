# mq2db

Dump messages from message queue into databae.

**NOTE: Currently it is using zmq only.**

This program connect to the target's MQ address.  
Received data is parsed and saved into database according to the configuration file.

## Configuration

Example configuration is here: [example.yaml](./example.yaml)
