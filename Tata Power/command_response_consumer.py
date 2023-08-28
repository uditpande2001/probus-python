import datetime
from kafka import KafkaConsumer
import json
import csv
import os
import logging
import colorlog
from datetime import date
from master.master_file import midnight_nodes

# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create color formatter
color_formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s",
    log_colors={
        'DEBUG': 'reset',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    },
    reset=True,
    style='%'
)

# Create console handler and set color formatter
console_handler = logging.StreamHandler()
console_handler.setFormatter(color_formatter)
# Add console handler to the logger
logger.addHandler(console_handler)

# Basic config for storing generated files
today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "Command_Response"
RESPONSE_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(RESPONSE_DIRECTORY):
    os.mkdir(RESPONSE_DIRECTORY)

file_path = os.path.join(RESPONSE_DIRECTORY, today)
if not os.path.exists(file_path):
    os.mkdir(file_path)


def kafka_consumer():
    consumer = KafkaConsumer(
        bootstrap_servers='tpwodl.probussense.com:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))

    )

    consumer.subscribe(['command-response'])

    for messages in consumer:
        node_id = messages.value['nodeId']
        if int(node_id) in midnight_nodes:
            command_type = str(messages.value['commandType'])

            status = str(messages.value['status'])
            file_name = f"{command_type}_{status}"

            print(node_id, datetime.datetime.now(), messages.value)

            if command_type == 'SET_METER_PASSWORD':
                with open(file_path + '\\' + file_name + '.csv', 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([node_id])

            if command_type == 'ENABLE_ALL':
                with open(file_path + '\\' + file_name + '.csv', 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([node_id])

            if command_type == 'DISABLE_ALL':
                with open(file_path + '\\' + file_name + '.csv', 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([node_id])

            status = str(messages.value['status'])
            command_ID = str(messages.value['commandId'])
            file_name = f"{command_type}_{status}"
            file_path = os.path.join(RESPONSE_DIRECTORY, today)
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

            if command_type == 'LIST_COMMANDS':
                print(messages.value, datetime.datetime.now())
                file_name = f"{command_type}"
                with open(file_path + '\\' + file_name + '.txt', 'a') as txtfile:
                    txtfile.write(str(messages.value) + str(datetime.datetime.now()) + "\n")


if __name__ == '__main__':
    kafka_consumer()
