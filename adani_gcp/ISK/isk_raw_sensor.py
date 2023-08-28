from kafka import KafkaConsumer
import json
from datetime import date
import os
from datetime import datetime
from ISK_master.ISK_master_file import list_of_nodes
import logging
import csv
import collections
now = datetime.now()
from_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
to_time = now.replace(hour=23, minute=59, second=59, microsecond=999)

today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "Raw_Sensor_Data"
DATA_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(DATA_DIRECTORY):
    os.mkdir(DATA_DIRECTORY)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

items = []
keys = []
values = []
time_msg = collections.OrderedDict()


def start_consuming():
    consumer = KafkaConsumer(
        bootstrap_servers='216.48.180.61:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    consumer.subscribe(['raw-sensor-data'])
    for messages in consumer:
        # if 'meterNumber' in messages.value.keys():
        node_id = messages.value['nodeId']
        # if int(node_id) == 417246 or int(node_id) == 400037:
        if int(node_id) in list_of_nodes:
            # print(messages.value)
            profile_type = messages.value['type']
            debug_string = messages.value['debugServerTime']
            # datetime_object = datetime.strptime(debug_string, '%Y-%m-%dT%H:%M:%S.%f')
            # print(type(datetime_object))
            # if from_time < datetime_object < to_time:

            if str(profile_type) == 'Instant_Profile':
                print(messages.value)
                meter_no = messages.value['meterNumber']
                meter_node = meter_no + '_' + str(node_id)
                # print(meter_node)
                file_name = f"{node_id}_{meter_no}"
                date_path = os.path.join(ABS_PATH, RELATIVE_PATH, today)
                if not os.path.exists(date_path):
                    os.mkdir(date_path)

                file_path = os.path.join(date_path, profile_type)
                if not os.path.exists(file_path):
                    os.mkdir(file_path)

                json_obj = json.dumps(messages.value, indent=4)
                with open(file_path + '\\' + file_name + '.json', 'a', newline='\n') as json_file:
                    json_file.write(json_obj)

                file = open('nagaland_nodes.csv', 'a', newline='')
                writer = csv.writer(file)
                writer.writerow([meter_node, debug_string, profile_type])
                # writer.writerow([])
                file.close()

            else:
                pass


if __name__ == '__main__':
    start_consuming()
