# Copyright 2023 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
from wirepas_mqtt_library import WirepasNetworkInterface
import wirepas_mesh_messaging as wmm
import logging
from time import sleep
from struct import unpack

from utils import RAPI_DEST_ENDPOINT, RAPI_SRC_ENDPOINT, build_change_node_address_command, get_node_address_change_status_from_csap_response, process_gateway_list
    
#################################################################################
# GLOBAL FUNCTIONS
#################################################################################    
def is_ping_response(status_payload):
    # We are only interested by ping responses
    response_type = unpack('<B', status_payload[0:1])[0]
    
    if response_type == 0x80:
        return True
  
    return False

def is_begin_response(status_payload):
    response_type = unpack('<B', status_payload[0:1])[0]
    
    if response_type == 0x81:
        return True
  
    return False

def on_data_received(data):
    global nodes_detected_with_given_address

    if data.source_endpoint == RAPI_DEST_ENDPOINT:
        if is_ping_response(data.data_payload):
            logging.info("Found node [%s] on gateway id [%s] and sink [%s]", data.source_address, data.gw_id, data.sink_id)
            nodes_detected_with_given_address+=1
            logging.info("Number of nodes found with node address [%s]: %s", data.source_address, nodes_detected_with_given_address)
        elif is_begin_response(data.data_payload):
            logging.info("Node address change status [%s]", get_node_address_change_status_from_csap_response(data.data_payload))
              

#################################################################################
# MAIN PROGRAM
#################################################################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--host',
                        default="someinstance",
                        help="MQTT broker address")
    parser.add_argument('--port', default=8883,
                        type=int,
                        help="MQTT broker port")
    parser.add_argument('--username', default='someusername',
                        help="MQTT broker username")
    parser.add_argument('--password',
                        default="somepassword",
                        help="MQTT broker password")
    parser.add_argument('--insecure',
                        dest='insecure',
                        action='store_true',
                        help="MQTT use unsecured connection")
    
    parser.add_argument('--network',
                        type=int,
                        help="Network address for which to get scratchpad status",
                        required=True
                        )

    parser.add_argument('--gateways',
                        type=process_gateway_list,
                        help='Specify the gateway under which the node is located. This script only supports one gateway')

    parser.add_argument('--sink_id',
                        help='Specify the sink id under which the node is located')

    parser.add_argument('--node_address',
                        help='Specify the node address to ping (in decimal)')
    
    parser.add_argument('--new_node_address',
                        help='The replacement address (in decimal) for the node address')
    
    parser.add_argument('--activation_delay_s',
                        type=int,
                        help="Activation delay (in sec) for the scratchpad activation command to be applied",
                        required=True)
    
    parser.add_argument('--delay_s',
                        type=int,
                        help="Specify how long the script should wait (in secs) for responses from nodes",
                        required=True)

    args = parser.parse_args()

    if (len(args.gateways) != 1):
        logging.error("Only one gateway must be specified for this script.")
        exit()
    else:
        gateway_id = args.gateways[0]

    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.INFO)

    wni = WirepasNetworkInterface(args.host,
                                  args.port,
                                  args.username,
                                  args.password,
                                  insecure=args.insecure)

    # whilst this variable is declared here, it is used in the data received callback function
    nodes_detected_with_given_address = 0

    # Register for any data
    wni.register_data_cb(on_data_received, network=args.network, gateway=gateway_id)
    
    sleep(10)

    # send a remote API ping request to detect if more than two nodes are under the same gateway
    try:
        payload = bytes.fromhex('0000')
        
        logging.info("Sending remote API ping command to detect nodes with [%s] on the network", args.node_address)

        # send remote API command.
        res = wni.send_message(gateway_id, args.sink_id, int(args.node_address, base=10), 255, 240, payload)
        if res != wmm.GatewayResultCode.GW_RES_OK:
             logging.error("Cannot send data to [%s]:[%s] res=[%s]", gateway_id, args.sink_id, res)

    except TimeoutError:
        logging.error("Cannot send data to [%s]:[%s]", gateway_id, args.sink_id)

    # allow time for the remote API ping response to be collected
    sleep(args.delay_s)

    if nodes_detected_with_given_address == 0:
        logging.error("Could not find the node [%s] on that gateway [%s] and sink [%s]", args.node_address, gateway_id, args.sink_id)
        exit()
    elif nodes_detected_with_given_address > 1:
        logging.error("Found more [%s] node [%s] on that gateway [%s] and sink [%s]", nodes_detected_with_given_address, args.node_address, gateway_id, args.sink_id)
        exit()
        
    # remote API to overwrite a node address    
    try:
        payload = bytes.fromhex(build_change_node_address_command(args.new_node_address, args.activation_delay_s))
        logging.info("Sending node address change command. Node [%s] will be changed to [%s]", args.node_address, args.new_node_address)
        # send remote API command.
        res = wni.send_message(gateway_id, args.sink_id, int(args.node_address, base=10), RAPI_SRC_ENDPOINT, RAPI_DEST_ENDPOINT, payload)
        if res != wmm.GatewayResultCode.GW_RES_OK:
             logging.error("Cannot send data to [%s]:[%s] res=[%s]", gateway_id, args.sink_id, res)
                   
    except TimeoutError:
        logging.error("Cannot send data to [%s]:[%s]", gateway_id, args.sink_id)

    sleep(args.delay_s)

    wni.close()