import time
import requests
import logging
from datetime import datetime, date, timedelta
from automated_commands.master.master_file import getConnectState_array
import csv
from kafka import KafkaConsumer
import json
import os
import threading

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

base_url = 'https://rf-adapter-prod.adanielectricity.com:443'

# CONSUMER CONFIG
today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "Command_Response"
Response_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(Response_DIRECTORY):
    os.mkdir(Response_DIRECTORY)

node_data = {}
node_count = len(getConnectState_array)
stop_flag = False


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


def getConnectState(node_id):
    try:
        node_data[node_id]['COMMAND GIVEN'] = datetime.now()
        url = base_url + "/command/getConnectState"
        header = {"Authorization": res_token}
        params = {
            'nodeId': node_id,
            'commandId': "%d" % round(time.time()),
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
        global node_count, stop_flag

        while not stop_flag:

            try:

                consumer = KafkaConsumer(
                    bootstrap_servers='10.127.4.99:9092',
                    auto_offset_reset='latest',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))

                )

                consumer.subscribe(['command-response'])

                for messages in consumer:

                    node_id = messages.value['nodeId']
                    if int(node_id) in getConnectState_array:

                        command_type = str(messages.value['commandType'])
                        if command_type == 'GET_CONNECT_STATE':

                            print(node_id, datetime.now(), messages.value)
                            status = str(messages.value['status'])

                            if status == "ACCEPTED":
                                node_data[node_id]['COMMAND ACCEPTED'] = datetime.now()

                            if status == "EXECUTED":
                                node_data[node_id]['COMMAND EXECUTED'] = datetime.now()

                    if node_count == 0:
                        print("command send to all nodes, stopping the consumer")
                        stop_flag = True
                        consumer.close()
                        time.sleep(10)

            except Exception as error:
                logging.error(error)


def write_to_csv():
    try:
        headers = ['node_id', 'COMMAND GIVEN', 'COMMAND ACCEPTED', 'COMMAND EXECUTED', 'ACCEPTED - GIVEN',
                   'EXECUTED - ACCEPTED'
                   ]

        file_path = os.path.join(Response_DIRECTORY, today)
        if not os.path.exists(file_path):
            os.mkdir(file_path)

        datestamp = datetime.now().strftime("%Y_%m_%d")
        file_name = f'get_status_test_file_{datestamp}'
        file_name = str(file_name)

        with open(file_path + '\\' + file_name + ".csv", 'a', newline='') as csvfile:

            writer = csv.writer(csvfile)
            writer.writerow([])
            writer.writerow([])
            writer.writerow(headers)

            for node in node_data.values():
                writer.writerow([node[key] for key in headers])

    except Exception as error:
        logging.info(error)

    finally:
        print("Program Stopped File is Generated")


if __name__ == '__main__':
    try:
        # signal.signal(signal.SIGINT, signal_handler)

        for node in getConnectState_array:
            node_data[node] = {

                'node_id': node,
                'COMMAND GIVEN': '',
                'COMMAND ACCEPTED': '',
                'COMMAND EXECUTED': '',
                'ACCEPTED - GIVEN': '',
                'EXECUTED - ACCEPTED': '',

            }

        print("starting consumer thread")
        consumer_thread = CRConsumer()
        consumer_thread.start()
        print("consumer thread started, wait for 10 seconds \n")
        time.sleep(10)

        for node in getConnectState_array:
            node_count -= 1
            getConnectState(node)
            print(f"nodes left {node_count}")
            if node_count == 0:
                print('COMMAND SEND TO ALL NODES')
            time.sleep(60)

        consumer_thread.join()

    except Exception as error:

        logging.error(error)

    finally:
        for node in node_data.values():
            t1 = node['COMMAND GIVEN']
            t2 = node['COMMAND ACCEPTED']
            t3 = node['COMMAND EXECUTED']

            if t1 and t2:
                time_diff_1 = t2 - t1
                node['ACCEPTED - GIVEN'] = time_diff_1
            else:
                time_diff_1 = None
                node['ACCEPTED - GIVEN'] = time_diff_1

            if t2 and t3:
                time_diff_2 = t3 - t2
                node['EXECUTED - ACCEPTED'] = time_diff_2
            else:
                time_diff_2 = None
                node['EXECUTED - ACCEPTED'] = time_diff_2

        print(node_data)
        write_to_csv()
