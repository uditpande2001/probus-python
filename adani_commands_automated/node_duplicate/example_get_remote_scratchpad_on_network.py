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
from utils import RAPI_DEST_ENDPOINT, RAPI_SRC_ENDPOINT, decode_msap_scratchpad_status, process_gateway_list

#################################################################################
# CONSTANTS
#################################################################################

# Send the scratchpad status request to a broadcast address so that all the nodes
# can reply and provide their status
BROADCAST = int(0xFFFFFFFF)
RAPI_COMMAND = bytes.fromhex("19 00")

#################################################################################
# GLOBAL VARIABLES
#################################################################################

# all scratchpad status are accumulated in this list as the responses are received and processed
stats = list()


#################################################################################
# GLOBAL FUNCTIONS
#################################################################################

def on_data_received(data):
    global stats
    response_elements_to_retain = ["seq", 'type', "status", "app_scr_seq", "app_version", "app_scr_len",
                                   "app_scr_crc", "app_area_id"]

    if data.source_endpoint == RAPI_DEST_ENDPOINT:
        logging.info("gw_id=[%s], sink_id=[%s], node_address=[%s], response=[%s]", data.gw_id, data.sink_id,
                     data.source_address, binascii.hexlify(data.data_payload))

        try:
            decoded_msap = decode_msap_scratchpad_status(data.data_payload, filter=response_elements_to_retain)
            logging.info("Decoded payload: %s", decoded_msap)
            file = open("fw_version.txt", 'a')
            file.write(str(data.gw_id) + ": " + str(data.sink_id) + ": " + str(data.source_address) + ": " +
                       str(decoded_msap["seq"]) + ": " + str(decoded_msap["type"]) + ": " + str(
                decoded_msap["status"]) + ": " +
                       str(decoded_msap["app_scr_seq"]) + ": " + str(decoded_msap["app_version"]) + ": " +
                       str(hex(decoded_msap["app_area_id"])) + ": " + str(hex(decoded_msap["app_scr_crc"])) + ": " +
                       str(hex(decoded_msap["app_scr_len"])) + "\n")
            entry_to_add = ";".join([str(data.gw_id), str(data.sink_id), str(data.source_address)])
            for decoded_element in decoded_msap:
                entry_to_add += ";" + str(decoded_msap[decoded_element])
            stats.append(entry_to_add)
        except Exception as e:
            logging.error("Caught exception when processing response: %s", str(e))


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
                        help="Network address for which to query the scratchpad")

    parser.add_argument('--gateways', default=None,
                        dest='gateways',
                        type=process_gateway_list,
                        help="Specify a comma separated list of gateways. If this parameter is ommited then remote API requests are sent to all the gateways on the specified network")

    parser.add_argument('--delay_s',
                        type=int,
                        help="Specify how long the script should wait (in secs) for responses from nodes")

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
    wni.register_data_cb(on_data_received,network=args.network)

    # Get the list of sinks currently detected for the specified network and gateway(s)
    # The get_sinks method will trigger await for the sink configuration to be retrieved from the sinks
    # for the gateway(s) that are online
    sinks = wni.get_sinks(network_address=args.network)

    # Send the scratchpad status request for all the
    try:
        for gw_id, sink_id, _ in sinks:
            logging.info("Sending scratchpad status request command to gw=[%s], sink=[%s]", gw_id, sink_id)
            res = wni.send_message(gw_id, sink_id, BROADCAST, RAPI_SRC_ENDPOINT, RAPI_DEST_ENDPOINT, RAPI_COMMAND)
            if res != wmm.GatewayResultCode.GW_RES_OK:
                print("Cannot send data to %s:%s res=%s" % (gw_id, sink_id, res))
    except Exception as e:
        logging.error("Caught exception when processing response: %s", str(e))

    sleep(args.delay_s)

    with open("./scratchpad_status_stats_1.csv", 'w') as f:
        f.write(
            "gw_id;sink_id;node_address;seq_number_present_on_node;action_type;scratchpad_status;seq_num_app_running_on_node\n")
        [f.write(entry + "\n") for entry in stats]

    wni.close()
