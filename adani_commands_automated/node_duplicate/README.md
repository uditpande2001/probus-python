# Installation

## Create a virtual environment and install the required packages

### Create a virtual environment
```
$ python3 -m venv venv
$ source venv/bin/activate
```

### Install the required packages from PyPi

```
$ pip install wirepas-mqtt-library more_itertools
```

## Introduction

This package provides examples of:
- a programmatic alternative to performing OTAP via WNT client (it covers legacy OTAP only).
- a programmatic way to modify node address remotely for a given network/gateway/sink.

It comprises of scripts covering the following phases of legacy OTAP:
- upload the scratchpad to gateways/sinks
  - upload the scratchpad file which causes the sinks to propagate the scratchpad to the network
- activate scratchpad update
  - instruct the nodes on the network to activate the stored scratchpad image
  - this script should be run after the the scratchpad have propagated in the network. For large networks, there should be sufficient time left for the propagation to take place prior to activating.
- query scratchpad status
  - query the nodes for scratchpad update status. Nodes reply with their stored scratchpad number and processed scratchpad number. Again, on large networks, sufficient time should be left between the activation and the status query.
- fix duplicate address
  - query the existence of the given node address on the given network/gateway/sink and update its node address if only one is found

## Recommended process for OTAP

It is highly recommended to follow the below procedure for any OTAP: 
1. Test the process and new application in a representative test environment to confirm a) the OTAP process through script for this scratchpad is performed successfully and b) the new application starts and works as expected.
2. We recommend that an update is done on production targetting one gateway only to verify that the update and application are working as expected.
3. Once single gateway is confirmed working, then updating the whole network can be performed in production.

## Recommended process for duplicate node address fixes
It is recommended that this script is trialed in development environment first. 

## Configuration files

For your convenience, there is a reference file that you can use to define the configuration: 
- dev_env: contains MQTT credentials for your development and testing instance
- prod_env: contains MQTT credentials for your production instance

These files can be used as arguments of the scripts and provide a convenient way to not specify each parameter in the command line invoking the script.

All OTAP scripts have been designed to operate in two modes:
- global: no filter on gateways. The operations apply on the whole network.
- targeted: upload scratchpad, activate scratchpad or query scratchpad status for specific gateways only. This mode has a more targeted approach and is recommended to be used first before moving to the global mode.

The configuration files provided operate in targeted mode by default. In this mode, one or more gateways can be specified (comma separated for multiple gateways)

To operate in global mode, simply remove the following lines from the configuration files: 

```
--gateways=861261056662485
```

## Get list of parameters

To get the list of parameters for a given script, invoke the script with -h option:

``` 
$ python3 example_upload_scratchpad_legacy_OTAP.py -h
usage: example_upload_scratchpad_legacy_OTAP.py [-h] --host HOST [--port PORT] --username USERNAME --password PASSWORD [--insecure] --network NETWORK [--gateways GATEWAYS] --scratchpad_seq_num SCRATCHPAD_SEQ_NUM [--file FILE]

optional arguments:
  -h, --help            show this help message and exit
  --host HOST           MQTT broker address
  --port PORT           MQTT broker port
  --username USERNAME   MQTT broker username
  --password PASSWORD   MQTT broker password
  --insecure            MQTT use unsecured connection
  --network NETWORK     Specify the network address for which to upload scratchpad
  --gateways GATEWAYS   Specify a comma separated list of gateways. If this parameter is ommited then the scratchpad is sent to all the gateways/sinks found on the specified network
  --scratchpad_seq_num SCRATCHPAD_SEQ_NUM
                        Scratchpad sequence number associated with the scratchpad to upload. Please take extra care in specifying the scratchpad number correctly based
  --file FILE           Specify the path to the scratchpad file to use (i.e. ~/scratchpad/application.otap)
```

## Upload scratchpad to network

To upload a scratchpad to a network, use the following script: 
``` 
$ python3 example_upload_scratchpad_legacy_OTAP.py @dev_env --scratchpad_seq_num <SN> --file <path_to_file> 

Parameters explanation:
--scratchpad_seq_num SCRATCHPAD_SEQ_NUM
                     Scratchpad sequence number associated with the scratchpad to upload. Please take extra care in specifying the scratchpad number correctly based
--file               FILE
                     Specify the path to the scratchpad file to use (i.e. ~/scratchpad/application.otap)

```

## Activate scratchpad update on network

To activate scraptchpad on a network, use the following script: 

``` 
$ python3 example_activate_legacy_OTAP.py @dev_env --delay_s DELAY_S --scratchpad_seq_num SCRATCHPAD_SEQ_NUM --activation_delay_s ACTIVATION_DELAY_S
  
Parameters explanation:
--delay_s DELAY_S    Specify how long the script should wait (in secs) for responses from nodes
--scratchpad_seq_num SCRATCHPAD_SEQ_NUM
                     Scratchpad sequence number to activate on the network
--activation_delay_s ACTIVATION_DELAY_S
                     Activation delay (in sec) for the scratchpad activation command to be applied

```

## Query scratchpad status on network

To query the scratchpad status of all the nodes for the specified network/gateways, use the following command:

``` 
$ python3 example_get_remote_scratchpad_on_network.py @dev_env --delay_s DELAY_S

Parameters explanation:
--delay_s DELAY_S    Specify how long the script should wait (in secs) for responses from nodes

```

## Duplicate addresses fix

To help with addressing the issue whereby duplicate node addresses are present on the network, use the following command: 
```
$ python3 example_fix_duplicate_address.py @dev_wnt --sink_id SINK_ID --node_address NODE_ADDR --new_node_address NEW_NODE_ADDR --activation_delay ACTIVATION_DELAY_S --delay_s DELAY_S

Parameters explanation:
--sink_id SINK_ID                       Specify the sink id under which the node is located
--node_address NODE_ADDRESS             Specify the node address to ping (in decimal)
--new_node_address NEW_NODE_ADDRESS     The replacement address (in decimal) for the node address
--activation_delay_s ACTIVATION_DELAY_S Activation delay (in sec) for the scratchpad activation command to be applied
--delay_s DELAY_S                       Specify how long the script should wait (in secs) for responses from nodes
```

## License

Licensed under the Apache License, Version 2.0.

