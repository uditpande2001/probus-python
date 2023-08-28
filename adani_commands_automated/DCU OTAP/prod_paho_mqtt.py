import csv
import datetime
import logging
import os
import json
import time

import paho.mqtt.client as mqtt
from paho.mqtt.client import ssl
#
# logging.basicConfig(filename='' level=logging.INFO,
#                     format="%(asctime)s : %(levelname)s : %(funcName)s :%(message)s")

# ABSOLUTE_PATH = os.path.dirname(__file__)
# dcu_wise_data = os.path.join(ABSOLUTE_PATH, '../dcu_wise_data')
# if not os.path.exists(dcu_wise_data):
#     os.mkdir(dcu_wise_data)
working_dir = os.getcwd()
# prod
broker = "nms-wirepass-prod.adanielectricity.com"
port = 8883
user = "mqttmasteruser"
password = "ENwQRmAOoCKG2QtAqYWNATqWKINU0Z"

# # dev
# broker = "nms-dev.adanielectricity.com"
# port = 8883
# user = "mqttmasteruser"
# password = "urDh7h5pRAwTcpehZCKp4DTvaVWVc"

# pgcil-isk
# broker = "pgcil-iskraemeco-wnt.probussense.com"
# port = 8883
# user = "mqttmasteruser"
# password = "fBD6L0jOSbhLL2H2AOlZvWcpdSw1gq"

# isk nagaland
# broker = "216.48.183.143"
# port = 1883
# user = 'mqttmasteruser'
# password = 'sqfjDxtrCN6wNMbp4mq2n2MaZ0m7vnN'

topic = [('hub-command-response', 0)]

payload = {
    "code": "19d69b3b-512f-499d-a0de-e6828d279df0",
    "commandId": 1233,
    "deviceId": "123",
    "commandType": "GET_RTC",
    "targetGwId": None,
    "commandDestination": "HUB",
    "properties": [{
        "propName": "2%&AtYHOZSeK$Cd4[raK^OZ)_",
        "propValue": "USB"
    }],
    "debug": False,
    "hideCommand": False
}


def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code : {rc}")
    client.subscribe(topic)
    print("Connected to MQTT broker")


def on_message(client, userdata, msg):
    data_topic = msg.topic
    byte_msg = msg.payload
    json_msg = byte_msg.decode('utf-8')
    msg_obj = json.loads(json_msg)
    print(msg_obj)
    if data_topic == "dcu-health-data":
        if 'dcu_type' in msg_obj:
            csv_filename = f"health_data.csv"
            with open(csv_filename, mode='a', newline='') as csv_file:
                csv_writer = csv.writer(csv_file)
                if csv_file.tell() == 0:
                    csv_writer.writerow(['dcu_type', 'hubUuid', 'battery_voltage', 'selectedSim', 'timestamp'])
                csv_writer.writerow([msg_obj['dcu_type'], msg_obj['hubUuid'], msg_obj['battery_voltage'], msg_obj['selectedSim'], msg_obj['timestamp']])
    elif data_topic == "hub-command-response":
        csv_filename = f"command_response.csv"
        with open(csv_filename, mode='a', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            if csv_file.tell() == 0:
                csv_writer.writerow(msg_obj.keys())
            csv_writer.writerow(msg_obj.values())
    elif data_topic == "dcu-outage-data":
        csv_filename = f"outage.csv"
        with open(csv_filename, mode='a', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            if csv_file.tell() == 0:
                csv_writer.writerow(msg_obj.keys())
            csv_writer.writerow(msg_obj.values())


client = mqtt.Client()
client.tls_set(tls_version=ssl.PROTOCOL_SSLv23)

client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(username=user, password=password)
client.connect(broker, port, 120)
client.loop_forever()
