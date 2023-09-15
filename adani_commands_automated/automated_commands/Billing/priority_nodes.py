import psycopg2
import requests
import logging
from time import time
import time
from datetime import datetime, timedelta, date
import threading
from kafka import KafkaConsumer
import json
import csv
import os

req_nodes = []
base_url = 'https://rf-adapter-prod.adanielectricity.com:443'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# making required directories:0
today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "Billing_Command_Response"
Response_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(Response_DIRECTORY):
    os.mkdir(Response_DIRECTORY)

RAW_SENSOR_PATH = "Billing_JSONS"
Response_DIRECTORY_2 = os.path.join(ABS_PATH, RAW_SENSOR_PATH)
if not os.path.exists(Response_DIRECTORY_2):
    os.mkdir(Response_DIRECTORY_2)

# -------------------------------------------------------------------------------------------
now = datetime.now()
billing_date_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
diag_time = now - timedelta(minutes=45)
print(billing_date_time, diag_time)

# importing priority nodes from given csv:
priority_nodes = []
with open('priority_nodes_list.csv', 'r', encoding='utf-8-sig') as file:
    csv_reader = csv.reader(file)
    for node in csv_reader:
        priority_nodes.append(int(node[0]))


def auth():
    try:
        url = base_url + '/auth/login'
        credential = {
            "password": "lAgRGmb8abCVfrBX",
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


def nodes_batch(node_list):
    try:
        url = base_url + '/config/getNodesBatch'

        nodeIds = node_list
        batch_size = {'batchSize': 1}
        head = {
            'Authorization': res_token
        }
        response = requests.post(url, json=nodeIds, params=batch_size, headers=head)
        if response.status_code == 200:
            # logging.info(response.text)
            return response.json()
        else:
            logging.error(response)
            logging.error(response.text)

    except Exception as error:
        print(error)


def billing(nodes):
    try:
        node_cmd = {}
        batch_array = nodes_batch(nodes)

        # getting number of nodes returned from batch_api
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

        num_elements, new_array = count_elements(batch_array)

        print("Total number of nodes:", num_elements)
        count = 0

        file = open("priority_send_nodes.csv", mode="a", newline="")
        writer = csv.writer(file)
        for array in batch_array:
            for node_id in array:
                count += 1

                print(f"billing command sent to {node_id} count = {count} nodes left = {num_elements - count}")

                try :
                    url = base_url + "/command/rfCommand"
                    headers = {"Authorization": res_token}
                    sensorCommand = {
                        'code': 'billing_test',
                        'commandDestination': "SENSOR",
                        'commandId': "%d" % round(time.time()),
                        'commandType': 'P_READ_BILLING',
                        'debug': True,
                        'deviceId': node_id,
                        'hideCommand': True,
                        'properties': [
                            {
                                'propName': 'P_COUNT',
                                'propValue': "1"
                            }]}
                    response = requests.post(url, json=sensorCommand, headers=headers)

                    logging.info(response.url)
                    if response.status_code == 200:
                        logging.info(response.text)
                        writer.writerow([node_id])
                    else:
                        logging.error(response)
                        logging.error(response.text)

                except Exception as error:
                    print(error)
            time.sleep(60)
        if count == num_elements:
            print('Command send to all required nodes ')
            file.close()

    except Exception as error:
        logging.error(error)


def get_db_data():
    try:

        print('connecting to database')
        db = psycopg2.connect(
            host='10.127.4.226',
            database='sensedb',
            user='postgres',
            password='probus@220706'

        )
        cursor = db.cursor()

        billing_query = f"""

                        SELECT DISTINCT node_id
                        FROM rf_diag
                        where server_time >='{diag_time}'
                        and  node_id NOT IN 
                                        (	SELECT DISTINCT node_id 
                                            FROM meter_profile_data mpd 
                                            WHERE date_time = ' {billing_date_time}'
                                            AND "type" = 'Billing_Profile');


           """

        print('executing billing query')
        cursor.execute(billing_query)
        results = cursor.fetchall()
        cursor.close()
        db.close()
        print('closed database connection')
        global req_nodes

        for result in results:
            if result[0] in priority_nodes:
                req_nodes.append(result[0])

        return req_nodes

    except Exception as error:
        print(error)


class CRConsumer(threading.Thread):
    daemon = True

    def run(self):

        consumer = KafkaConsumer(
            bootstrap_servers='10.127.4.99:9092',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))

        )

        consumer.subscribe(['command-response'])

        for messages in consumer:
            node_id = messages.value['nodeId']
            if int(node_id) in req_nodes:

                # print(datetime.now(),messages.value)
                command_type = str(messages.value['commandType'])
                if command_type == 'P_READ_BILLING':
                    print(datetime.now(), messages.value)
                    status = str(messages.value['status'])

                    file_name = f"{command_type}_{status}"

                    file_path = os.path.join(Response_DIRECTORY, today)
                    if not os.path.exists(file_path):
                        os.mkdir(file_path)

                    with open(file_path + '\\' + file_name + '.csv', 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        # time_send = datetime.datetime.now()
                        writer.writerow([node_id])

                    with open(file_path + '\\' + file_name + 'full_message.csv', 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        # time_send = datetime.datetime.now()
                        writer.writerow([messages.value])


class RawSensorConsumer(threading.Thread):
    daemon = True

    def run(self):

        consumer = KafkaConsumer(
            bootstrap_servers='10.127.4.99:9092',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        consumer.subscribe(['raw-sensor-data'])
        for messages in consumer:
            if 'meterNumber' in messages.value.keys():
                node_id = messages.value['nodeId']

                if int(node_id) in req_nodes:
                    profile_type = messages.value['type']
                    if str(profile_type) == 'Billing_Profile':
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

    nodes = get_db_data()

    threads = [
        CRConsumer(),
        RawSensorConsumer()
    ]
    for t in threads:
        t.start()
    time.sleep(10)
    billing(nodes)

    for t in threads:
        t.join()
