import datetime
from kafka import KafkaConsumer
import json
import csv
import os
import logging
from datetime import date,datetime

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "../Command_Response"
Response_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(Response_DIRECTORY):
    os.mkdir(Response_DIRECTORY)


def start_consuming():
    consumer = KafkaConsumer(
        bootstrap_servers='10.127.2.7',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))

    )

    consumer.subscribe(['command-response'])


    for messages in consumer:
        print(datetime.now(),messages.value)




        # node_id = messages.value['nodeId']
        # # if int(node_id) in ConnectDisconnect_array:
        # if int(node_id) == 435495:
        #
        #     command_type = str(messages.value['commandType'])
        #     # if command_type == 'CONNECT_DISCONNECT':
        #     print(node_id,datetime.datetime.now(),messages.value)
        #
        #     status = str(messages.value['status'])
        #     command_ID = str(messages.value['commandId'])
        #     file_name = f"{command_type}_{status}"
        #     file_path = os.path.join(Response_DIRECTORY, today)
        #     if not os.path.exists(file_path):
        #         os.mkdir(file_path)
                    #
                # with open(file_path + '\\' + file_name + '.csv', 'a', newline='') as csvfile:
                #     writer = csv.writer(csvfile)
                #     # time_send = datetime.datetime.now()
                #     writer.writerow([node_id])
                #
                # with open(file_path + '\\' + file_name + 'full_message.csv', 'a', newline='') as csvfile:
                #     writer = csv.writer(csvfile)
                #     # time_send = datetime.datetime.now()
                #     writer.writerow([messages.value])

                # if command_type == 'LIST_COMMANDS':
                #     print(messages.value, datetime.datetime.now())
                #     file_name = f"{command_type}"
                #     with open(file_path + '\\' + file_name + '.txt', 'a') as txtfile:
                #         txtfile.write(str(messages.value) + str(datetime.datetime.now()) + "\n")


if __name__ == '__main__':
    start_consuming()
