import json
import logging
import threading
import time
from datetime import date
import datetime
import os
from kafka import KafkaConsumer
# from master import dev_master

ABSOLUTE_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "data"
DATA_DIRECTORY = os.path.join(ABSOLUTE_PATH, RELATIVE_PATH)
if not os.path.exists(DATA_DIRECTORY):
    os.mkdir(DATA_DIRECTORY)



def correct_incorrect(message):
    if message.value.get('commandId') is None:
        if message.value['type'] != 'Event_Profile':
            if len(message.value['dataObis']) == 0 or len(message.value['data'][0]) == 0 or len(
                    message.value['scalarObis']) == 0 or len(message.value['scalar']) == 0:
                print(f'Incorrect  {str(message.value)}')
                # file_test_blank = open(ABSOLUTE_PATH + '\\' + 'Office_test_blank_data' + '.txt', 'a')
                # file_test_blank.write(node_id + "_" + meterNumber + "_" + Profile_type + "_" + TOR + '\n')

            else:
                print(f'Correct  {str(message.value)}')
        else:
            print(f'Correct  {str(message.value)}')
    else:
        if message.value['type'] != 'Event_Profile':
            if 'meterNumber' not in message.value.keys() or message.value['meterNumber'] == '' or len(
                    message.value['dataObis']) == 0 or len(message.value['data'][0]) == 0 or len(
                message.value['scalarObis']) == 0 or len(message.value['scalar']) == 0:

                print(f'on Demand : Incorrect  {str(message.value)}')
            else:
                print(f'on Demand : Correct  {str(message.value)}')
        else:
            print(f'on Demand : Correct  {str(message.value)}')


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        print("raw-sensor-data topic...")
        consumer = KafkaConsumer(bootstrap_servers='10.127.2.7:9092',
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        consumer.subscribe(['raw-sensor-data'])
        try:
            for message in consumer:
                node_id = str(message.value['nodeId'])
                # epoch_start = datetime.datetime(2023, 5, 10, 0, 0, 0)
                # epoch_end = datetime.datetime(2023, 5, 10, 23, 59, 59)
                TimeStamp = datetime.date.fromtimestamp(message.timestamp // 1000)
                # print(TimeStamp)
                directory_path = os.path.join(DATA_DIRECTORY, str(TimeStamp))
                if not os.path.exists(directory_path):
                    os.mkdir(directory_path)
                folder_name_profile = str(message.value['type'])
                if not os.path.exists(directory_path + '\\' + folder_name_profile):
                    os.mkdir(directory_path + '\\' + folder_name_profile)

                # if epoch_start <= TimeStamp <= epoch_end:
                # if int(node_id) in dev_master.test_node:
                f = open(directory_path + '\\' + folder_name_profile + '\\' + node_id + '.txt', 'a')
                f.write(str(message.value) + '\n')
                f.close()
                correct_incorrect(message)
                # print(TimeStamp)

        except Exception as error:
            print(error)


def main():
    threads = [
        Consumer()
    ]
    for t in threads:
        t.start()
        time.sleep(10)
    while True:
        pass


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    try:
        main()
    except Exception as e:
        print(e)
    finally:
        print('\nConsumer stopped')
