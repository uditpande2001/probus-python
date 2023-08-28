from kafka import KafkaConsumer
import json
from datetime import date
import os
from datetime import datetime
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
inst_count = collections.OrderedDict()
inst_time = collections.OrderedDict()
node_mtr = collections.OrderedDict()

try:
    def start_consuming():
        consumer = KafkaConsumer(
            bootstrap_servers='10.127.4.99:9092',
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        consumer.subscribe(['raw-sensor-data'])

        for messages in consumer:
            # if 'meterNumber' in messages.value.keys():
            node_id = messages.value['nodeId']

            if int(node_id) == 710430:
                global node_mtr

                profile_type = messages.value['type']
                debug_string = messages.value['debugServerTime']
                datetime_object = datetime.strptime(debug_string, '%Y-%m-%dT%H:%M:%S.%f')

                if from_time <= datetime_object <= to_time:

                    if str(profile_type) == 'Event_Profile':
                        print(messages.value)

                        # meter_node = meter_no + '_' + str(node_id)


                    else:
                        pass
except Exception as error:
    logging.error(error)

if __name__ == '__main__':
    try:
        start_consuming()

    except Exception as error:
        logging.error(error)
    finally:
        pass
