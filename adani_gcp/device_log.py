import datetime
import logging
import os
import threading
import time
import colorlog
from kafka import KafkaConsumer

dir_name = os.getcwd()

# config for storing csv files
today = str(datetime.date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "DCU LOGS"
RESPONSE_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(RESPONSE_DIRECTORY):
    os.mkdir(RESPONSE_DIRECTORY)

file_path = os.path.join(RESPONSE_DIRECTORY, today)
if not os.path.exists(file_path):
    os.mkdir(file_path)

# logger config

# logging.basicConfig(
#     format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
#            '%(levelname)s:%(process)d:%(message)s',
#     level=logging.INFO
# )


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

console_handler = logging.StreamHandler()
console_handler.setFormatter(color_formatter)
logger.addHandler(console_handler)


class ConsumerSecond(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='10.127.4.99:9092',
                                 auto_offset_reset='latest',
                                 value_deserializer=lambda m: m)
        # # dev
        # consumer = KafkaConsumer(bootstrap_servers='10.127.2.7:9092',
        #                          auto_offset_reset='latest',
        #                          value_deserializer=lambda m: m)
        # # pgcil_isk
        # consumer = KafkaConsumer(bootstrap_servers='pgcil-iskraemeco.probussense.com:9092',
        #                          auto_offset_reset='latest',
        #                          value_deserializer=lambda m: m)

        consumer.subscribe(['device-logs'])
        for message in consumer:
            gw_id = message.key
            print(str(gw_id), datetime.datetime.now())
            try:
                with open(dir_name + f'/{gw_id}.txt', 'ab') as f:
                    f.write(message.value)

                with open(file_path + f'/{gw_id}.txt', 'ab') as file:
                    file.write(message.value)
            except Exception as e:
                print(e)


def main():
    threads = [
        ConsumerSecond()
    ]
    for t in threads:
        t.start()
        time.sleep(10)
    while True:
        pass


if __name__ == "__main__":

    try:
        main()
    except Exception as e:
        print(e)
    finally:
        print('\nConsumer stopped')
