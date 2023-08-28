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
inst_count = collections.OrderedDict()
inst_time = collections.OrderedDict()
node_mtr = collections.OrderedDict()

try:
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
            meter_no = messages.value['meterNumber']
            if int(node_id) in list_of_nodes:
                global node_mtr
                node_mtr[node_id] = meter_no
                # print(node_mtr)

                profile_type = messages.value['type']
                debug_string = messages.value['debugServerTime']
                datetime_object = datetime.strptime(debug_string, '%Y-%m-%dT%H:%M:%S.%f')

                if from_time <= datetime_object <= to_time:

                    if str(profile_type) == 'Instant_Profile':
                        print(messages.value)

                        # meter_node = meter_no + '_' + str(node_id)
                        global inst_count
                        global inst_time

                        if node_id in inst_count.keys():
                            instant_count = inst_count[node_id]
                            instant_count += 1
                            inst_count[node_id] = instant_count

                        else:
                            inst_count[node_id] = 1

                        if node_id in inst_time:
                            inst_time[node_id].append(debug_string)
                        else:
                            inst_time[node_id] = [debug_string]

                        file_name = f"{node_id}_{meter_no}"

                        # writing to json ---------------------------------------
                        date_path = os.path.join(ABS_PATH, RELATIVE_PATH, today)
                        if not os.path.exists(date_path):
                            os.mkdir(date_path)

                        file_path = os.path.join(date_path, profile_type)
                        if not os.path.exists(file_path):
                            os.mkdir(file_path)

                        json_obj = json.dumps(messages.value, indent=4)
                        with open(file_path + '\\' + file_name + '.json', 'a', newline='\n') as json_file:
                            json_file.write(json_obj)

                        with open('testing_nagaland.csv', mode='w', newline='') as csv_file:
                            writer = csv.writer(csv_file)
                            writer.writerow(['Node ID', 'METER NO', 'PROFILE', 'Count', 'Instant Time'])

                            # Write inst_count and inst_time to CSV
                            for node_id in list_of_nodes:
                                if node_id in inst_count:
                                    count = inst_count[node_id]
                                else:
                                    count = 'no_data'

                                if node_id in node_mtr:
                                    meter = node_mtr[node_id]
                                else:
                                    meter = 'no_data'

                                if node_id in inst_time:
                                    time_str = ', '.join(inst_time[node_id])
                                else:
                                    time_str = 'no_data'

                                writer.writerow([node_id, meter, profile_type, count, time_str])

                        # file = open('nagaland_nodes.csv', 'a', newline='')
                        # writer = csv.writer(file)
                        # writer.writerow([meter_node, debug_string, profile_type])
                        # # file.close()
                        # writing to csv ------------------------------------------------------
                        # with open('testing_nagaland.csv', mode='w', newline='') as csv_file:
                        #     writer = csv.writer(csv_file)
                        #     writer.writerow(['Node ID','PROFILE', 'Count', 'Instant Time'])
                        #
                        #     # Write inst_count and inst_time to CSV
                        #     for node_id, count in inst_count.items():
                        #         if node_id in inst_time:
                        #             writer.writerow([node_id,profile_type, count, ', '.join(inst_time[node_id])])
                        #         else:
                        #             writer.writerow([node_id, count, ''])
                        #
                        #     for node_id in inst_time.keys():
                        #         if node_id not in inst_count:
                        #             writer.writerow([node_id, '', ', '.join(inst_time[node_id])])

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
