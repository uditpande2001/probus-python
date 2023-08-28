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
COMMAND_RESPONSE_PATH = "Instant_Command_Response"
RAW_SENSOR_PATH = "Instant_JSONS"

Response_DIRECTORY_1 = os.path.join(ABS_PATH, COMMAND_RESPONSE_PATH)
if not os.path.exists(Response_DIRECTORY_1):
    os.mkdir(Response_DIRECTORY_1)

Response_DIRECTORY_2 = os.path.join(ABS_PATH, RAW_SENSOR_PATH)
if not os.path.exists(Response_DIRECTORY_2):
    os.mkdir(Response_DIRECTORY_2)

now = datetime.now()
date_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
required_diag_time = now - timedelta(hours=4)
yesterday = now - timedelta(days=1)
previous_day_server_time = yesterday.replace(hour=22, minute=0, second=0, microsecond=0)
today_limit = now.replace(hour=23, minute=59, second=59, microsecond=999)

db = None
cursor = None

abs_path = os.path.dirname(__file__)
directory = os.path.join(abs_path, "data")

# config for api calls including epoch time creation
base_url = 'http://216.48.180.61:9999'
current_epoch_time = int(time.time())
from_time_obj = datetime.strptime('00:00:00', '%H:%M:%S')
to_time_obj = datetime.strptime('23:59:59', '%H:%M:%S')
from_time = from_time_obj.time()
to_time = to_time_obj.time()
epoch_from = int(datetime.combine(datetime.today(), from_time).timestamp())
epoch_to = int(datetime.combine(datetime.today(), to_time).timestamp())

print('from epoch ', epoch_from, flush=True)
print('to epoch ', epoch_to, flush=True)

server_time_to = date_time.now()
server_time_from = date_time.now() - timedelta(hours=2)
print(server_time_to)
print(server_time_from)

total_nodes = []


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


def instant(node_id):
    try:
        url = base_url + "/command/rfCommand"
        headers = {"Authorization": res_token}
        sensorCommand = {
            'code': 'instant_test',
            'commandDestination': "SENSOR",
            'commandId': "%d" % round(time.time()),
            'commandType': 'P_READ_INSTANT',
            'debug': True,
            'deviceId': node_id,
            "hideCommand": True,
            'properties': []
        }
        response = requests.post(url, json=sensorCommand, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def get_db_data():
    try:
        print("\n\n\n", flush=True)
        print('connecting to database', flush=True)
        db = psycopg2.connect(
            host='216.48.180.61',
            database='sensedb',
            user='postgres',
            password='probus@220706'

        )
        cursor = db.cursor()
        instant_query = f"""
                            SELECT DISTINCT node_id
                            FROM rf_diag rd
                            WHERE server_time >='{server_time_from}' AND node_id >=400000
                            AND node_id NOT IN (
                            SELECT node_id
                            FROM meter_profile_data mpd
                            WHERE server_time BETWEEN '{server_time_from}' AND '{server_time_to}'
                            AND "type" = 'Instant_Profile'
                            GROUP BY node_id)


                           """
        print("running query", flush=True)
        cursor.execute(instant_query)
        results = cursor.fetchall()
        global total_nodes

        for node in results:
            total_nodes.append(node[0])

        print(total_nodes)

        db.close()
        return total_nodes

    except Exception as error:
        print(error, flush=True)
    finally:
        if cursor is not None:
            print("database connection closed", flush=True)
            print("\n", flush=True)

            cursor.close()
        if db is not None:
            db.close()


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
                print(messages.value, datetime.now())
                command_type = str(messages.value['commandType'])

                if command_type == 'P_READ_INSTANT':
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
                # if int(node_id) == 402201:
                if int(node_id) in total_nodes:
                    profile_type = messages.value['type']
                    if str(profile_type) == 'Instant_Profile':
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


def send_command():
    TOTAL_NODES = get_db_data()
    time.sleep(5)

    total_nodes = len(TOTAL_NODES)
    print(f'TOTAL NODES = {total_nodes}', flush=True)
    print("\n", flush=True)

    for node in TOTAL_NODES:
        total_nodes -= 1
        instant(node)
        print(f'instant command send to {node}  remaining nodes = {total_nodes} ', flush=True)
        time.sleep(10)


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
        send_command()

        for t in threads:
            t.join()

        print("All threads completed")

    except Exception as error:
        print(error)
