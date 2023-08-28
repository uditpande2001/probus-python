# Copyright 2023 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
import time

from wirepas_mqtt_library import WirepasNetworkInterface
import wirepas_mesh_messaging as wmm
import logging
from time import sleep
from struct import unpack
import binascii

from utils import RAPI_DEST_ENDPOINT, RAPI_SRC_ENDPOINT, build_change_node_network_address_and_channel, is_network_address_channel_channel_successful
    
#################################################################################
# GLOBAL FUNCTIONS
#################################################################################    
def is_ping_response(status_payload):
    # We are only interested in ping responses
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
        else:
            if is_network_address_channel_channel_successful(data.data_payload):
                logging.info("Changed network address/channel [%s] successfully", binascii.hexlify(data.data_payload).decode())
            else:
                logging.info("Failed to change network address/channel [%s]", binascii.hexlify(data.data_payload).decode())

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
                        help="Current network address of the node",
                        required=True
                        )

    parser.add_argument('--gateway_id',
                        help='Current gateway of the node',
                        required=True)

    parser.add_argument('--sink_id',
                        help='Current sink id of the node',
                        required=True)

    parser.add_argument('--node_address',
                        help='Specify the node address for which to change network (in decimal)',
                        required=True)
    
    parser.add_argument('--new_network_address',
                        help='The replacement network address (in decimal) for the node',
                        required=True)
    
    parser.add_argument('--new_network_channel',
                        help='The replacement network channel (in decimal) for the node',
                        required=True)

    parser.add_argument('--activation_delay_s',
                        type=int,
                        help="Activation delay (in sec) for the remote API command to be applied",
                        required=True)
    
    parser.add_argument('--delay_s',
                        type=int,
                        help="Specify how long the script should wait (in secs) for responses from nodes",
                        required=True)

    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.INFO)

    wni = WirepasNetworkInterface(args.host,
                                  args.port,
                                  args.username,
                                  args.password,
                                  insecure=args.insecure
                                  )
    # time.sleep(10)
    # whilst this variable is declared here, it is used in the data received callback function
    nodes_detected_with_given_address = 0

    # Register for any data
    wni.register_data_cb(on_data_received, network=args.network, gateway=args.gateway_id)

    sleep(10)

    # send a remote API ping request to detect if more than two nodes are under the same gateway
    try:
        payload = bytes.fromhex('0000')

        logging.info("Sending remote API ping command to detect nodes with [%s] on the network", args.node_address)

        # send remote API command.
        res = wni.send_message(args.gateway_id, args.sink_id, int(args.node_address, base=10), 255, 240, payload)
        if res != wmm.GatewayResultCode.GW_RES_OK:
             logging.error("Cannot send data to [%s]:[%s] res=[%s]", args.gateway_id, args.sink_id, res)

    except TimeoutError:
        logging.error("Cannot send data to [%s]:[%s], timeout error", args.gateway_id, args.sink_id)

    # allow time for the remote API ping response to be collected
    sleep(args.delay_s)

    if nodes_detected_with_given_address == 0:
        logging.error("Could not find the node [%s] on that gateway [%s] and sink [%s]", args.node_address, args.gateway_id, args.sink_id)
        exit()
    elif nodes_detected_with_given_address > 1:
        logging.error("Found more [%s] node [%s] on that gateway [%s] and sink [%s]", nodes_detected_with_given_address, args.node_address, args.gateway_id, args.sink_id)
        exit()

    # remote API to overwrite a network channel and network address
    # try:
    #     payload = bytes.fromhex(build_change_node_network_address_and_channel(args.new_network_address, args.new_network_channel, args.activation_delay_s))
    #     logging.info("Sending network address change command. Network address [%s] will be changed to [%s] with channel [%s] on node [%s]", args.network, args.new_network_address, args.new_network_channel, args.node_address)
    #     logging.info("Remote API payload: [%s]", binascii.hexlify(payload))
    #     # send remote API command.
    #     res = wni.send_message(args.gateway_id, args.sink_id, int(args.node_address, base=10), RAPI_SRC_ENDPOINT, RAPI_DEST_ENDPOINT, payload)
    #     if res != wmm.GatewayResultCode.GW_RES_OK:
    #          logging.error("Cannot send data to [%s]:[%s] res=[%s]", args.gateway_id, args.sink_id, res)
    #
    # except TimeoutError:
    #     logging.error("Cannot send data to [%s]:[%s]", args.gateway_id, args.sink_id)
    #
    # sleep(args.delay_s)
    #
    # wni.close()
