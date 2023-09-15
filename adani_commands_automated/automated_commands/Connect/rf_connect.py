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

# making required directories:
today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "Connect_Command_Response"
Response_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(Response_DIRECTORY):
    os.mkdir(Response_DIRECTORY)

# -------------------------------------------------------------------------------------------
now = datetime.now()


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


def get_db_data(meter):
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
                            SELECT node_id, max(server_time)					
                            FROM rf_diag rd 
                            WHERE node_id in( 	
                                            SELECT node_id 
                                            FROM meter_mapping mm WHERE meter_number = '{meter}')
                            GROUP BY node_id
           """

        print('executing connect meter  query')
        cursor.execute(billing_query)
        results = cursor.fetchall()
        cursor.close()
        db.close()
        print('closed database connection \n')
        global req_nodes
        for result in results:
            req_nodes.append(result[0])
        print(meter, result[0], " last diag- ", result[1])
        return req_nodes

    except Exception as error:
        print(error)


def connect(node_id, command_id):
    try:
        url = base_url + "/command/connectDisconnect"
        header = {"Authorization": res_token}
        params = {
            'nodeId': node_id,
            # 'commandId': "%d" % round(time.time()),
            'commandId': {command_id},
            'state': 'CONNECTED',
            'mode': 'MODE_NONE',
            'meterMaker': 'GENUS',

        }
        response = requests.post(url=url, params=params, headers=header)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)

    except requests.exceptions.HTTPError as error:
        logging.error(error)


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

                print(datetime.now(), messages.value)
                status = str(messages.value['status'])
                command_ID = str(messages.value['commandId'])
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


if __name__ == '__main__':

    threads = [
        CRConsumer(),
    ]
    for t in threads:
        t.start()

    time.sleep(5)

    while True:
        meter_no = input("Enter the meter no ")
        meter_no = meter_no.upper().replace(" ", "")

        nodes = get_db_data(meter_no)

        command_id = input("Enter the command ID ")
        command_id = command_id.replace(" ", "")

        flag = input("enter y to send the command any other key to cancel ")
        if flag == 'y' or flag == 'Y':
            connect(nodes[0], command_id)
        else:
            print("wrong input try again \n ")


    for t in threads:
        t.join()

#
# //SM10162451
