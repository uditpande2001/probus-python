from kafka import KafkaConsumer
import json
import os
import logging
from datetime import date
import collections
import matplotlib.pyplot as plt
import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "Command_Response"
Response_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(Response_DIRECTORY):
    os.mkdir(Response_DIRECTORY)

time_msg = collections.OrderedDict()
items = []
keys = []
values = []
try:
    def start_consuming():
        consumer = KafkaConsumer(
            bootstrap_servers='10.127.4.99:9092',
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))

        )

        consumer.subscribe(['rf-data'])

        for messages in consumer:

            # print(messages.value['serverTime'])
            # print(len(messages.value['serverTime']))
            # print(messages)
            # print(type(messages))
            # print(messages.value['serverTime'])
            time = messages.value['serverTime'][11:21]
            # print(time)
            if time in time_msg.keys():
                count = time_msg[time]
                count += 1
                time_msg[time] = count
                # print(time_msg)

            else:
                time_msg[time] = 1


    # keys = list(time_msg.keys())
    # values = list(time_msg.values())


except Exception as error:
    print(error)


if __name__ == '__main__':

    try:
        start_consuming()

    except Exception as error:
        logging.error(error, exc_info=True)
    finally:
        # keys = list(time_msg.keys())
        # values = list(time_msg.values())
        # print(time_msg)
        # plt.plot(keys, values)
        # plt.xlabel('TIME')
        # plt.ylabel('NO OF PACKETS RECEIVED')
        # plt.xticks(rotation=90)
        # plt.show()

        data = pd.DataFrame(list(time_msg.items()), columns=['timestamp', 'count'])
        data['timestamp'] = pd.to_datetime(data['timestamp'])
        # data.set_index('timestamp', inplace=True)
        grouped_data = data.groupby(pd.Grouper(key='timestamp', freq='1S')).mean()  # use mean for avg of count
        plt.plot(grouped_data.index, grouped_data['count'])
        plt.xlabel('Time')
        plt.xticks(rotation=90)
        plt.ylabel('Count')
        plt.show()

        # data = pd.DataFrame(list(time_msg.items()), columns=['timestamp', 'count'])
        # data['timestamp'] = pd.to_datetime(data['timestamp'])
        # grouped_data = data.groupby(pd.Grouper(key='timestamp', freq='1S')).mean()
        #
        # fig, ax = plt.subplots(figsize=(10, 6))  # set the figure size to 10x6 inches
        # ax.plot(grouped_data.index, grouped_data['count'])
        # ax.set_xlabel('Time', fontsize=12)  # set the font size of the x-axis label to 12
        # ax.tick_params(axis='x', rotation=90)  # rotate the x-axis tick labels by 90 degrees
        # ax.set_ylabel('Count', fontsize=12)
        # plt.show()


