import datetime
from kafka import KafkaConsumer
import json
import csv
import os
import logging
from datetime import date
from ISK.ISK_master.ISK_master_file import list_of_nodes



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "Command_Response"
Response_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(Response_DIRECTORY):
    os.mkdir(Response_DIRECTORY)


def start_consuming():
    consumer = KafkaConsumer(
        bootstrap_servers='216.48.180.61:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))

    )

    consumer.subscribe(['command-response'])

    for messages in consumer:

        node_id = messages.value['nodeId']
        if int(node_id) in list_of_nodes:
            print(messages.value)



        # if int(node_id) == 403514:
        #     print(messages.value, datetime.datetime.now())
        #     command_type = str(messages.value['commandType'])
        #     status = str(messages.value['status'])
        #     file_name = f"{command_type}_{status}"
        #     file_path = os.path.join(Response_DIRECTORY, today)
        #     if not os.path.exists(file_path):
        #         os.mkdir(file_path)
        #
        #     with open(file_path + '\\' + file_name + '.csv', 'a', newline='') as csvfile:
        #         writer = csv.writer(csvfile)
        #         # time_send = datetime.datetime.now()
        #         writer.writerow([node_id])
        #
        #     if command_type == 'LIST_COMMANDS':
        #         print(messages.value, datetime.datetime.now())
        #         file_name = f"{command_type}"
        #         with open(file_path + '\\' + file_name + '.txt', 'a') as txtfile:
        #             txtfile.write(str(messages.value) + str(datetime.datetime.now()) + "\n")

            print(messages.value, datetime.datetime.now())
            command_type = str(messages.value['commandType'])
            status = str(messages.value['status'])
            file_name = f"{command_type}_{status}"
            # file_path = os.path.join(Response_DIRECTORY, today)
            # if not os.path.exists(file_path):
            #     os.mkdir(file_path)
            #
            # with open(file_path + '\\' + file_name + '.csv', 'a', newline='') as csvfile:
            #     writer = csv.writer(csvfile)
            #     # time_send = datetime.datetime.now()
            #     writer.writerow([node_id])
            #
            # if command_type == 'LIST_COMMANDS':
            #     print(messages.value, datetime.datetime.now())
            #     file_name = f"{command_type}"
            #     with open(file_path + '\\' + file_name + '.txt', 'a') as txtfile:
            #         txtfile.write(str(messages.value) + str(datetime.datetime.now()) + "\n")



if __name__ == '__main__':
    start_consuming()
