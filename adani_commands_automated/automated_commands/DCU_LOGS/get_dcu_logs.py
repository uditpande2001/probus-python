import os
import time
import logging
import colorlog
import requests
import threading
from datetime import datetime, date

from kafka import KafkaConsumer

BASE_URL = 'https://rf-adapter-prod.adanielectricity.com:443'

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
logger.addHandler(console_handler)

# config for storing csv files
today = str(date.today())
ABS_PATH = os.path.dirname(__file__)

file_path = os.path.join(ABS_PATH, today)
if not os.path.exists(file_path):
    os.mkdir(file_path)


def auth():
    try:
        credential = {
            "password": "lAgRGmb8abCVfrBX",
            "userId": "probus"
        }
        url = BASE_URL + '/auth/login'
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


def getHubLogs(gw_id):
    try:
        url = BASE_URL + "/command/getHubLogs"
        headers = {"Authorization": token}
        params = {
            'gwId': gw_id,
            'commandId': "%d" % round(time.time())
        }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def hubLogs(gw_id, start_time, end_time):
    try:
        url = BASE_URL + "/command/hubLogs"
        headers = {"Authorization": token}
        params = {
            'gwId': gw_id,
            'startTime': start_time,
            'endTime': end_time
        }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


class consumer(threading.Thread):

    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='10.127.4.99:9092',
                                 auto_offset_reset='latest',
                                 value_deserializer=lambda m: m)
        # # dev
        # consumer = KafkaConsumer(bootstrap_servers='10.127.2.7:9092',
        #                          auto_offset_reset='latest',
        #                          value_deserializer=lambda m: m)
        # # pgcil_isk
        # consumer = KafkaConsumer(bootstrap_servers='pgcil-iskraemeco.probussense.com:9092',
        #                          auto_offset_reset='latest',
        #                          value_deserializer=lambda m: m)

        consumer.subscribe(['device-logs'])
        for message in consumer:
            gw_id = message.key
            print(str(gw_id), datetime.now())
            try:
                with open(file_path + f'/{gw_id}.txt', 'ab') as file:
                    file.write(message.value)
            except Exception as e:
                print(e)


if __name__ == '__main__':

    # list_of_gws = ['861261056654888', '861261056662675', '866340057557012']
    list_of_gws = ['866340057573167', '866340057541941', '866340057550488',
                   '866340057563614', '866340057566559', '866340057573126', '866340057573365']
    global_counter = 1

    start_time = "2023-06-25 00:00:00"
    end_time = "2023-06-26 00:00:00"

    print(" staring consumer wait for 10 seconds")
    threads = [
        consumer()
    ]
    for t in threads:
        t.start()
        time.sleep(10)

    for gw_id in list_of_gws:
        getHubLogs(gw_id)
        hubLogs(gw_id, start_time, end_time)
        print(f"Command sent to : {gw_id} : Counter {global_counter}")
        global_counter += 1
        time.sleep(5)

    for t in threads:
        t.join()
