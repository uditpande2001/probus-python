import logging
import os
import threading
import time

from kafka import KafkaConsumer

ABSOLUTE_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "DCU_LOGS"
RELATIVE_PATH_1 = "TOR"

DATA_DIRECTORY = os.path.join(ABSOLUTE_PATH, RELATIVE_PATH)
if not os.path.exists(DATA_DIRECTORY):
    os.mkdir(DATA_DIRECTORY)
DATA_DIRECTORY_1 = os.path.join(ABSOLUTE_PATH, RELATIVE_PATH_1)
if not os.path.exists(DATA_DIRECTORY_1):
    os.mkdir(DATA_DIRECTORY_1)
dir_name = os.getcwd()
# print(dir_name)


class ConsumerSecond(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='10.127.4.99:9092',
                                 auto_offset_reset='earliest',
                                 value_deserializer=lambda m: m.decode('utf-8'))
        consumer.subscribe(['device-logs'])
        for message in consumer:
            print(str(message.key.decode("utf-8")))
            # print(str(message.value))

            with open(dir_name + f'/{message.key.decode("utf-8")}.txt', 'a') as f:
                f.write(message.value)


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
