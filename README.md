# mq2db

Dump data from message queue into database.

**NOTE: Currently it is using zmq only.**

This program connect to the target's MQ address.  
Received data is parsed and saved into database according to the configuration file.

## Configuration

Example configuration is here: [example.yaml](./example.yaml)  
More examples can also be found in [tests](./tests/) directory.
