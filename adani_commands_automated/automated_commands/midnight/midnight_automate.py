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
COMMAND_RESPONSE_PATH = "Command_Response"
RAW_SENSOR_PATH = "Midnight_JSONS"

Response_DIRECTORY_1 = os.path.join(ABS_PATH, COMMAND_RESPONSE_PATH)
if not os.path.exists(Response_DIRECTORY_1):
    os.mkdir(Response_DIRECTORY_1)

Response_DIRECTORY_2 = os.path.join(ABS_PATH, RAW_SENSOR_PATH)
if not os.path.exists(Response_DIRECTORY_2):
    os.mkdir(Response_DIRECTORY_2)

COMMAND_SEND_TOTAL_PATH = "Total_send"

Total_send_dir = os.path.join(ABS_PATH, COMMAND_SEND_TOTAL_PATH,today)
if not os.path.exists(Total_send_dir):
    os.mkdir(Total_send_dir)

file_name = 'total_midnight_sent' + str(time.time())

now = datetime.now()
date_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
required_diag_time = now - timedelta(minutes=45)

print(f'date_time = {date_time} diag_time = {required_diag_time}', flush=True)

db = None
cursor = None

abs_path = os.path.dirname(__file__)
directory = os.path.join(abs_path, "data")

# config for api calls including epoch time creation
base_url = 'https://rf-adapter-prod.adanielectricity.com:443'
current_epoch_time = int(time.time())
from_time_obj = datetime.strptime('00:00:00', '%H:%M:%S')
to_time_obj = datetime.strptime('23:59:59', '%H:%M:%S')
from_time = from_time_obj.time()
to_time = to_time_obj.time()
epoch_from = int(datetime.combine(datetime.today(), from_time).timestamp())
epoch_to = int(datetime.combine(datetime.today(), to_time).timestamp())
print('from epoch ', epoch_from, flush=True)
print('to epoch ', epoch_to, flush=True)
print("\n", flush=True)

midnight_nodes = []
with open('nodes_list.csv', 'r', encoding='utf-8-sig') as file:
    csv_reader = csv.reader(file)
    for node in csv_reader:
        midnight_nodes.append(int(node[0]))


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


def midnight(node_id):
    try:

        print(f'command send to {node_id}', flush=True)
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

    except Exception as error:
        logging.error(error)


def get_db_data():
    try:
        print("\n\n\n", flush=True)
        print('connecting to database', flush=True)
        db = psycopg2.connect(
            host='10.127.4.226',
            database='sensedb',
            user='postgres',
            password='probus@220706'

        )
        cursor = db.cursor()
        midnight_query = f"""   select distinct node_id
                                from rf_diag rd 
                                where server_time >='{required_diag_time}' and node_id >= 400000
                                and end_point like '%253/255'
                                and node_id not in (
                                                    select distinct node_id
                                                    from meter_profile_data mpd 
                                                    where date_time ='{date_time}'
                                                    and "type" = 'Midnight_Profile')


                           """
        print("running query", flush=True)
        cursor.execute(midnight_query)
        results = cursor.fetchall()
        total_nodes = []

        for node in results:
            total_nodes.append(node[0])

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
            bootstrap_servers='10.127.4.99:9092',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))

        )

        consumer.subscribe(['command-response'])

        for messages in consumer:
            node_id = messages.value['nodeId']
            if int(node_id) in midnight_nodes:
                print(messages.value, datetime.now())
                command_type = str(messages.value['commandType'])
                status = str(messages.value['status'])
                command_ID = str(messages.value['commandId'])
                file_name = f"{command_type}_{status}"
                file_path = os.path.join(Response_DIRECTORY_1, today)
                if not os.path.exists(file_path):
                    os.mkdir(file_path)

                with open(file_path + '\\' + file_name + '.csv', 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    # time_send = datetime.datetime.now()
                    writer.writerow([node_id,command_ID])

                with open(file_path + '\\' + file_name + 'full_message.csv', 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    # time_send = datetime.datetime.now()
                    writer.writerow([messages.value])

                if command_type == 'LIST_COMMANDS':
                    print(messages.value, datetime.datetime.now())
                    file_name = f"{command_type}"
                    with open(file_path + '\\' + file_name + '.txt', 'a') as txtfile:
                        txtfile.write(str(messages.value) + str(datetime.datetime.now()) + "\n")



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
                # if int(node_id) == 402201:
                if int(node_id) in midnight_nodes:
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
    #  midnight but have given diag in the last 4 hours
    TOTAL_NODES = get_db_data()
    time.sleep(5)

    required_nodes = []
    for node in midnight_nodes:
        if node in TOTAL_NODES:
            required_nodes.append(node)

    total_nodes = len(required_nodes)
    print(f'TOTAL NODES = {required_nodes}', flush=True)
    print(f'TOTAL NODES = {total_nodes}', flush=True)
    print("\n", flush=True)

    batches = nodes_batch(required_nodes)



    file = open(Total_send_dir + '\\' + file_name + '.csv', 'a', newline='')
    writer = csv.writer(file)
    writer.writerow(['node_id','command_send_time'])

    for node_array in batches:
        for node in node_array:
            time_send = datetime.now()
            writer.writerow([node,time_send])
            total_nodes -= 1
            midnight(node)

            print(f'midnight command send to {node}  remaining nodes = {total_nodes} ', flush=True)

        time.sleep(15)
        print('\n', flush=True)


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

        send_command()

        for t in threads:
            t.join()

        print("All threads completed")

    except Exception as error:
        print(error)
