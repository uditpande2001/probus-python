# Copyright 2023 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
import wirepas_mesh_messaging as wmm
import logging
import binascii

from wirepas_mqtt_library import WirepasNetworkInterface
from time import sleep
from datetime import datetime
from utils import RAPI_DEST_ENDPOINT, RAPI_SRC_ENDPOINT, process_gateway_list, build_otap_activation_command, get_activate_status_from_msap_response

#################################################################################
# CONSTANTS
#################################################################################

# Send the scratchpad status request to a broadcast address so that all the nodes
# can reply and provide their status
BROADCAST = int(0xFFFFFFFF)

#################################################################################
# GLOBAL VARIABLES
#################################################################################

# all scratchpad status are accumulated in this list as the responses are received and processed
stats = list()

# record the time at which the remote API activation command is sent
start_time = None

#################################################################################
# GLOBAL FUNCTIONS
#################################################################################

def on_data_received(data):
    global stats
    global start_time

    if data.source_endpoint == RAPI_DEST_ENDPOINT:
        # calculate the delay between the activation request and the response from any given node
        delay = datetime.utcnow() - start_time
        logging.info("gw_id=[%s], sink_id=[%s], node_address=[%s], response=[%s], delay=[%s]", data.gw_id, data.sink_id, data.source_address, binascii.hexlify(data.data_payload), delay)
        print(data.data_payload)
        stats.append(";".join([str(data.gw_id), str(data.sink_id), str(data.source_address), binascii.hexlify(data.data_payload).decode(), str(delay), get_activate_status_from_msap_response(data.data_payload)]))

#################################################################################
# MAIN PROGRAM
#################################################################################

if __name__ == "__main__":
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--host',
                        help="MQTT broker address",
                        required=True)
    parser.add_argument('--port', default=8883,
                        type=int,
                        help="MQTT broker port")
    parser.add_argument('--username',
                        help="MQTT broker username",
                        required=True)
    parser.add_argument('--password',
                        help="MQTT broker password",
                        required=True)
    
    parser.add_argument('--insecure',
                        dest='insecure',
                        action='store_true',
                        help="MQTT use unsecured connection")
    
    parser.add_argument('--network',
                        type=int,
                        help="Network address for which to get scratchpad status",
                        required=True
                        )
    
    parser.add_argument('--gateways', default=None,
                        dest='gateways',
                        type=process_gateway_list,
                        help="Specify a comma separated list of gateways. If this parameter is ommited then remote API requests are sent to all the gateways/sinks found on the specified network")
    
    parser.add_argument('--delay_s',
                        type=int,
                        help="Specify how long the script should wait (in secs) for responses from nodes",
                        required=True)

    parser.add_argument('--scratchpad_seq_num',
                        type=int,
                        help="Scratchpad sequence number to activate on the network",
                        required=True)
    
    parser.add_argument('--activation_delay_s',
                        type=int,
                        help="Activation delay (in sec) for the scratchpad activation command to be applied",
                        required=True)

    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.INFO)

    wni = WirepasNetworkInterface(args.host,
                                  args.port,
                                  args.username,
                                  args.password,
                                  insecure=args.insecure,
                                  strict_mode=False)

    
    # Register a callback to process the remote API responses with scratchpad status from the nodes
    # The on_data_received callback filters out any messages that are not from the network and gateway(s) specified
    wni.register_data_cb(on_data_received, gateway=args.gateways, network=args.network)

    # Get the list of sinks currently detected for the specified network and gateway(s)
    # The get_sinks method will trigger await for the sink configuration to be retrieved from the sinks
    # for the gateway(s) that are online
    sinks = wni.get_sinks(network_address=args.network, gateway=args.gateways)

    rapi_activation_cmd = build_otap_activation_command(args.scratchpad_seq_num, args.activation_delay_s)

    start_time = datetime.utcnow()

    payload = bytes.fromhex(rapi_activation_cmd)
    logging.info("Command used to activate scratchpad: %s", binascii.hexlify(payload))
    
    # send remote API command to online gateways only
    for gw_id, sink_id, _ in sinks:
        logging.info("Sending activation command to gw=[%s], sink=[%s]", gw_id, sink_id)
        
        res = wni.send_message(gw_id, sink_id, BROADCAST, RAPI_SRC_ENDPOINT, RAPI_DEST_ENDPOINT, payload)
    
        if res != wmm.GatewayResultCode.GW_RES_OK:
            logging.info("Cannot send data to gw=[%s], sink=[%s] res=[%s]", gw_id, sink_id, res)

    sleep(args.delay_s)

    with open("./otap_activate_stats.csv",'w') as f:
        f.write("gw_id;sink_id;node_address;response;delay;activation_status\n")
        [f.write(entry + "\n") for entry in stats]

    wni.close()