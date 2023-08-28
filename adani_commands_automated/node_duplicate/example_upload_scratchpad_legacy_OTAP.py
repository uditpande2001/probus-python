# Copyright 2023 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
import wirepas_mesh_messaging as wmm
import logging

from wirepas_mqtt_library import WirepasNetworkInterface
from utils import process_gateway_list


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
                        help="Specify the network address for which to upload scratchpad",
                        required=True
                        )
    
    parser.add_argument('--gateways', default=None,
                        dest='gateways',
                        type=process_gateway_list,
                        help="Specify a comma separated list of gateways. If this parameter is ommited then the scratchpad is sent to all the gateways/sinks found on the specified network")
    
    parser.add_argument('--scratchpad_seq_num',
                        type=int,
                        help="Scratchpad sequence number associated with the scratchpad to upload. Please take extra care in specifying the scratchpad number correctly based",
                        required=True)
    
    parser.add_argument('--file',
                    help="Specify the path to the scratchpad file to use (i.e. ~/scratchpad/application.otap)",
                    )
    
    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.INFO)

    wni = WirepasNetworkInterface(args.host,
                                  args.port,
                                  args.username,
                                  args.password,
                                  insecure=args.insecure,
                                  strict_mode=False)

    # Get the list of sinks currently detected for the specified network and gateway(s)
    # The get_sinks method will trigger await for the sink configuration to be retrieved from the sinks
    # for the gateway(s) that are online
    sinks = wni.get_sinks(network_address=args.network, gateway=args.gateways)

    # Read the scratchpad provided as parameter
    try:
        with open(args.file, "rb") as f:
            scratchpad_file = f.read()
    except Exception as e:
        logging.error("Cannot read scratchpad file: [%s] " % str(e))
        exit()

    # all scratchpad upload status are accumulated in this list as the responses are received and processed
    stats = list()
    
    # send remote API command to online gateways only
    for gw_id, sink_id, _ in sinks:
        logging.info("Uploading scratchpad to gw=[%s], sink=[%s]", gw_id, sink_id)
        
        res = wni.upload_scratchpad(gw_id, sink_id, args.scratchpad_seq_num, scratchpad_file)

        stats.append(";".join([str(gw_id), str(sink_id), str(args.scratchpad_seq_num), str(res)]))

        if res != wmm.GatewayResultCode.GW_RES_OK:
            logging.error("Cannot send data to gw=[%s], sink=[%s] res=[%s]", gw_id, sink_id, res)
        else:
            logging.info("Successfully uploaded scratchpad with sequence number [%s] to gw=[%s], sink=[%s] res=[%s]", args.scratchpad_seq_num, gw_id, sink_id, res)

    with open("./upload_scratchpad_stats.csv",'w') as f:
        f.write("gw_id;sink_id;seq_number;upload_to_sink_response\n")
        [f.write(entry + "\n") for entry in stats]

    wni.close()
