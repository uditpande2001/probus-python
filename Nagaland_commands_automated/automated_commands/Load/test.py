import time
import psycopg2
import os
from datetime import datetime, timedelta, date
import logging
import requests
import csv
import json
from kafka import KafkaConsumer
import threading

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
COMMAND_RESPONSE_PATH = "Load_Command_Response"
RAW_SENSOR_PATH = "Load_JSONS"

Response_DIRECTORY_1 = os.path.join(ABS_PATH, COMMAND_RESPONSE_PATH)
if not os.path.exists(Response_DIRECTORY_1):
    os.mkdir(Response_DIRECTORY_1)

Response_DIRECTORY_2 = os.path.join(ABS_PATH, RAW_SENSOR_PATH)
if not os.path.exists(Response_DIRECTORY_2):
    os.mkdir(Response_DIRECTORY_2)

db = None
cursor = None

# config for api calls including epoch time creation
base_url = 'http://216.48.180.61:9999'

current_datetime = datetime.now()
# Subtract one day to get the previous day's date
previous_day_datetime = current_datetime - timedelta(days=1)
# Set the time to midnight (00:00)
midnight_datetime = previous_day_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
# Set the time to 23:59:59.000
end_of_day_datetime = previous_day_datetime.replace(hour=23, minute=59, second=59, microsecond=0)
# Convert the datetimes to epoch time
epoch_from = int(midnight_datetime.timestamp())
epoch_to = int(end_of_day_datetime.timestamp())

print(epoch_from)
print(epoch_to)

total_nodes = [404211, 400235]


def auth():
    try:
        url = base_url + '/auth/login'
        credential = {
            "password": "probus@123",
            "userId": "probus"
        }
        response = requests.post(url=url, json=credential)
        logging.info(response.url)

        if response.status_code == 200:
            token = response.text
            logging.info(token)
            return token

        else:
            logging.error(response)
            logging.error(response.text)
            return None
    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


res_token = auth()


def load(node_id):
    url = base_url + '/command/rfCommand'

    sensorCommand = {'code': "load_test",
                     'commandDestination': "SENSOR",
                     'commandId': "%d" % round(time.time()),
                     'commandType': "P_READ_LOAD",
                     'debug': True,
                     'deviceId': node_id,
                     'hideCommand': True,
                     'properties': [
                         {
                             "propName": "P_FROM",
                             "propValue": f'{epoch_from}',
                         },
                         {
                             "propName": "P_TO",
                             "propValue": f'{epoch_to}'
                         }
                     ]
                     }
    head = {'Authorization': res_token}
    response = requests.post(url=url, json=sensorCommand, headers=head)
    if response.status_code == 200:
        logging.info(response)
        logging.info(response.text)
    else:
        logging.error(response)
        logging.error(response.text)


class CRConsumer(threading.Thread):
    daemon = True

    def run(self):

        consumer = KafkaConsumer(
            bootstrap_servers='216.48.180.61:9092',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))

        )

        consumer.subscribe(['command-response'])

        for messages in consumer:
            node_id = messages.value['nodeId']
            if int(node_id) in total_nodes:

                command_type = str(messages.value['commandType'])

                if command_type == 'P_READ_LOAD':
                    print(messages.value, datetime.now())
                    status = str(messages.value['status'])
                    command_ID = str(messages.value['commandId'])
                    file_name = f"{command_type}_{status}"
                    file_path = os.path.join(Response_DIRECTORY_1, today)
                    if not os.path.exists(file_path):
                        os.mkdir(file_path)

                    with open(file_path + '\\' + file_name + '.csv', 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        # time_send = datetime.datetime.now()
                        writer.writerow([node_id, command_ID])

                    with open(file_path + '\\' + file_name + 'full_message.csv', 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        # time_send = datetime.datetime.now()
                        writer.writerow([messages.value])

                    # if command_type == 'LIST_COMMANDS':
                    #     print(messages.value, datetime.datetime.now())
                    #     file_name = f"{command_type}"
                    #     with open(file_path + '\\' + file_name + '.txt', 'a') as txtfile:
                    #         txtfile.write(str(messages.value) + str(datetime.datetime.now()) + "\n")


class RawSensorConsumer(threading.Thread):
    daemon = True

    def run(self):

        consumer = KafkaConsumer(
            bootstrap_servers='216.48.180.61:9092',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        consumer.subscribe(['raw-sensor-data'])

        for messages in consumer:
            if 'meterNumber' in messages.value.keys():
                node_id = messages.value['nodeId']

                if int(node_id) in total_nodes:
                    profile_type = messages.value['type']
                    if str(profile_type) == 'Load_Profile':
                        # print(messages.value, datetime.now())
                        meter_no = messages.value['meterNumber']
                        file_name = f"{node_id}_{meter_no}"
                        date_path = os.path.join(ABS_PATH, Response_DIRECTORY_2, today)
                        if not os.path.exists(date_path):
                            os.mkdir(date_path)

                        file_path = os.path.join(date_path, profile_type)
                        if not os.path.exists(file_path):
                            os.mkdir(file_path)

                        json_obj = json.dumps(messages.value, indent=4)
                        with open(file_path + '\\' + file_name + '.json', 'a', newline='\n') as json_file:
                            json_file.write(json_obj)
                else:
                    pass


if __name__ == '__main__':

    try:

        threads = [
            CRConsumer(),
            RawSensorConsumer()
        ]
        for t in threads:
            t.start()
            # while True:
            #     pass

        time.sleep(10)

        for node in total_nodes:
            load(node)

        for t in threads:
            t.join()

        print("All threads completed")

    except Exception as error:
        print(error)
