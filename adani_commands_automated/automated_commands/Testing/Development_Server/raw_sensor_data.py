from kafka import KafkaConsumer
import json
from datetime import date
import os
from datetime import datetime
import logging
import csv
from automated_commands.master.master_file import event_array

today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "Raw_Sensor_Data"
DATA_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(DATA_DIRECTORY):
    os.mkdir(DATA_DIRECTORY)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def start_consuming():
    consumer = KafkaConsumer(bootstrap_servers='10.127.2.7:9092',
                             auto_offset_reset='earliest',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    consumer.subscribe(['raw-sensor-data'])

    file = open('4th_AUG_JSON.csv','a')
    csv_writer = csv.writer(file)
    csv_writer.writerow([])
    for messages in consumer:
        node_id = messages.value['nodeId']
        profile_type = messages.value['type']
        meter_no = messages.value['meterNumber']
        # if meter_no == '9025061':
        file_name = f"{node_id}_{meter_no}"
        if profile_type == 'Event_Profile':
            csv_writer.writerow([meter_no,messages.value])
            print(messages.value)
            date_path = os.path.join(ABS_PATH, RELATIVE_PATH, today)
            if not os.path.exists(date_path):
                os.mkdir(date_path)

            file_path = os.path.join(date_path, profile_type)
            if not os.path.exists(file_path):
                os.mkdir(file_path)


            if profile_type == 'Event_Profile':

                json_obj = json.dumps(messages.value, indent=4)
                with open(file_path + '\\' + file_name + '.json', 'a', newline='\n') as json_file:
                    json_file.write(json_obj)



            # if int(node_id) in billing_array:
            #     profile_type = messages.value['type']
            #     if str(profile_type) == 'Billing_Profile':
            #         # print(messages.value)
            #         print(messages.value, datetime.now())
            #         meter_no = messages.value['meterNumber']
            #         file_name = f"{node_id}_{meter_no}"
            #         date_path = os.path.join(ABS_PATH, RELATIVE_PATH, today)
            #         if not os.path.exists(date_path):
            #             os.mkdir(date_path)
            #
            #         file_path = os.path.join(date_path, profile_type)
            #         if not os.path.exists(file_path):
            #             os.mkdir(file_path)
            #
            #         json_obj = json.dumps(messages.value, indent=4)
            #         with open(file_path + '\\' + file_name + '.json', 'a', newline='\n') as json_file:
            #             json_file.write(json_obj)
            # else:
            #     pass


if __name__ == '__main__':
    start_consuming()
