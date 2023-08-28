import datetime
import json
import logging
import colorlog
import requests
import threading
from kafka import KafkaConsumer
from datetime import date
import os
import csv

today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
# RELATIVE_PATH = "../Testing/Development_Server/send_password_dir"
RELATIVE_PATH = "send_password_dir"

DATA_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(DATA_DIRECTORY):
    os.mkdir(DATA_DIRECTORY)

BASE_URL = 'https://rf-adapter-dev.adanielectricity.com:9999'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create color formatter
color_formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s",
    log_colors={
        'DEBUG': 'reset',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    },
    reset=True,
    style='%'
)
console_handler = logging.StreamHandler()
console_handler.setFormatter(color_formatter)

# Add console handler to the logger
logger.addHandler(console_handler)
count = 0


def auth():
    try:
        credential = {
            "password": "kBgRGmb9abCVfrAT",
            "userId": "probus"
        }
        url = BASE_URL + '/auth/login'
        print(url)
        response = requests.post(url=url, json=credential)
        logging.info(response.url)
        if response.status_code == 200:
            res_token = response.text
            logging.info(response)
            logging.info(response.text)
            return res_token
        else:
            logging.error(response)
            logging.error(response.text)
            return None
    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


token = auth()


def sendMeterPassword(node_id, gw_id, sink_id, meter_maker):
    # password = None
    if meter_maker == "GO":
        password = 'AeMlHlSugaPl01ab'
    elif meter_maker == 'EH':
        password = 'ABCD0001'
    elif meter_maker == 'LN':
        password = 'lnt1'
    else:
        password = '11111111'
    print(node_id, gw_id, sink_id, password)

    try:
        url = BASE_URL + "/command/sendMeterPassword"
        headers = {"Authorization": token}
        params = {
            'nodeId': node_id,
            'gwId': gw_id,
            'sinkId': sink_id
        }
        meterPassword = {
            "authKey": "",
            "dedicatedKey": "",
            "encryptKey": "",
            "password": password,
            "systemTitle": ""
        }

        response = requests.post(url, json=meterPassword, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


class NodeInitConsumer(threading.Thread):
    daemon = True

    def run(self):
        print("node-init-response...")
        consumer = KafkaConsumer(bootstrap_servers='10.127.4.99:9092',
                                 auto_offset_reset='latest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['node-init-response'])


        for message in consumer:
            node_id = message.value['nodeId']

            meterNumber = message.value['meterNumber']
            Gw_id = message.value['gwId']
            sink_id = message.value['sinkId']
            meter_maker = message.value['meterMaker']
            # check node id
            # if int(node_id) in password_array:
                # if int(node_id) == 465407 or int(node_id) == 465623 or int(node_id) == 451809  :
            date_path = os.path.join(ABS_PATH, RELATIVE_PATH, today)
            global count
            count += 1
            if not os.path.exists(date_path):
                os.mkdir(date_path)
            if meter_maker is None or meter_maker == ' ':
                print(node_id, meterNumber, 'meter_maker is None')
                with open(date_path + '\\' + 'none_meter.csv', 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([node_id, meterNumber, Gw_id, meter_maker, datetime.datetime.now()])
            else:
                meter_maker = meter_maker[0:2]
                print(node_id, meterNumber, meter_maker, Gw_id, count)
                sendMeterPassword(node_id, Gw_id, sink_id, meter_maker)
                with open(date_path + '\\' + 'command_sent_details.csv', 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([node_id, meterNumber, Gw_id, meter_maker, datetime.datetime.now()])


if __name__ == '__main__':
    threads = [
        NodeInitConsumer()
    ]
    for t in threads:
        t.start()
        while True:
            pass
