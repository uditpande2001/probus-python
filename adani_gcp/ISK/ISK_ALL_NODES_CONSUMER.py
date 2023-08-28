from kafka import KafkaConsumer
import json
from datetime import date, timedelta
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

node_mtr = collections.OrderedDict()

temp_time = from_time - timedelta(days=1)

try:
    def start_consuming():
        consumer = KafkaConsumer(
            bootstrap_servers='216.48.180.61:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        consumer.subscribe(['raw-sensor-data'])

        for messages in consumer:
            debug_string = messages.value['debugServerTime']
            datetime_object = datetime.strptime(debug_string, '%Y-%m-%dT%H:%M:%S.%f')

            profile_type = messages.value['type']

            if from_time <= datetime_object <= to_time:

                if str(profile_type) == 'Instant_Profile':
                    node_id = messages.value['nodeId']
                    meter_no = messages.value['meterNumber']
                    # if int(node_id) == 400379:
                    print(messages.value)
                    if node_id in node_mtr.keys():
                        if meter_no not in node_mtr[node_id]:
                            node_mtr[node_id].append(meter_no)
                        else:
                            pass
                    else:
                        node_mtr[node_id] = [meter_no]

                    with open('nagaland_node_meter.csv', mode='w', newline='') as csv_file:
                        writer = csv.writer(csv_file)
                        writer.writerow(['Node ID','METER NO' ])

                        for node_id, string_values in node_mtr.items():
                            writer.writerow([node_id, ",".join(string_values)])







except Exception as error:
    logging.error(error)

if __name__ == '__main__':
    try:
        start_consuming()

    except Exception as error:
        logging.error(error)
    finally:
        pass
