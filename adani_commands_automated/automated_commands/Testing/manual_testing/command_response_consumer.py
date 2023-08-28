import datetime
from kafka import KafkaConsumer
import json
import csv
import os
import logging
from datetime import date
from automated_commands.master.master_file import ConnectDisconnect_array

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
    try:
        consumer = KafkaConsumer(
            bootstrap_servers='10.127.2.7',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))

        )

        consumer.subscribe(['command-response'])
        count = 0
        for messages in consumer:
            node_id = messages.value['nodeId']
            if int(node_id) in ConnectDisconnect_array:
            # if int(node_id) == 630213 :
                    # or node_id == 433567 or node_id == 432825:

                command_type = str(messages.value['commandType'])
                if command_type == 'CONNECT_DISCONNECT':
                    count +=1
                    print(node_id,datetime.datetime.now(),count, messages.value)

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

                    # if command_type == 'LIST_COMMANDS':
                    #     print(messages.value, datetime.datetime.now())
                    #     file_name = f"{command_type}"
                    #     with open(file_path + '\\' + file_name + '.txt', 'a') as txtfile:
                    #         txtfile.write(str(messages.value) + str(datetime.datetime.now()) + "\n")

    except Exception as error:
        logging.info(error)
    finally:
        print("consumer stopped")


if __name__ == '__main__':
    start_consuming()
