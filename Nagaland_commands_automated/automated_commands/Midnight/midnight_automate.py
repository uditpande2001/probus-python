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
COMMAND_RESPONSE_PATH = "Midnight_Command_Response"
RAW_SENSOR_PATH = "Midnight_JSONS"

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
previous_day_datetime = current_datetime - timedelta(days=1)
midnight_datetime = previous_day_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
end_of_day_datetime = previous_day_datetime.replace(hour=23, minute=59, second=59, microsecond=0)
epoch_from = int(midnight_datetime.timestamp())
epoch_to = int(end_of_day_datetime.timestamp())

print(epoch_from)
print(epoch_to)

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


def midnight(node_id):
    url = base_url + '/command/rfCommand'

    sensorCommand = {'code': "midnight_test",
                     'commandDestination': "SENSOR",
                     'commandId': "%d" % round(time.time()),
                     'commandType': "P_READ_MIDNIGHT",
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
        midnight_query = f"""							
                                SELECT DISTINCT node_id 
                                FROM rf_diag rd 
                                WHERE server_time >= '{current_datetime - timedelta(minutes=45)}'
                                AND node_id >= 400000
                                AND node_id NOT IN 
                                                    (
                                                    SELECT DISTINCT node_id 
                                                    FROM meter_profile_data mpd 
                                                    WHERE date_time >= '{midnight_datetime}' 
                                                    AND date_time <= '{end_of_day_datetime}' 
                                                    AND "type" = 'Midnight_Profile'
                                                    )
"""
        print("running query", flush=True)
        cursor.execute(midnight_query)
        results = cursor.fetchall()
        global total_nodes

        for result in results:
            total_nodes.append(result[0])

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


def nodes_batch(node_list):
    url = base_url + '/config/getNodesBatch'

    nodeIds = node_list
    batch_size = {'batchSize': 1}
    head = {
        'Authorization': res_token
    }
    response = requests.post(url, json=nodeIds, params=batch_size, headers=head)
    if response.status_code == 200:
        logging.info(response.text)
        return response.json()
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

                if command_type == 'P_READ_MIDNIGHT':
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
                    if str(profile_type) == 'Midnight_Profile':
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
    nodes_batch_array = nodes_batch(TOTAL_NODES)
    time.sleep(5)

    def count_elements(arr):
        count = 0
        elements_array = []
        for element in arr:
            if isinstance(element, list):
                nested_count, nested_elements = count_elements(
                    element)  # Recursively count elements in nested array
                count += nested_count
                elements_array.extend(nested_elements)
            else:
                count += 1  # Found a single element
                elements_array.append(element)  # Add the element to the new array
        return count, elements_array

    num_elements, new_array = count_elements(nodes_batch_array)

    print("Total number of nodes:", num_elements)

    file = open('Midnight_Sent_nodes.csv', 'w', newline='')
    for node_array in nodes_batch_array:
        for node in node_array:
            midnight(node)
            num_elements -= 1
            file.write(str(node))
            file.write("\n")
            print(f'midnight command send to {node}  remaining nodes = {num_elements} ', flush=True)

        time.sleep(10)
    if num_elements == 0:
        print("Command send to all the required nodes")

    file.close()


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
