from kafka import KafkaConsumer
import json
import logging
import psycopg2
import threading
import time
from datetime import datetime
from queue import Queue
import colorlog

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

connection = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="12345",
    host="localhost",
    port="5432"
)
cursor = connection.cursor()

data_queue = Queue()


def bulk_insert():
    while True:
        time.sleep(10)
        data_to_insert = []

        while not data_queue.empty():
            data_to_insert.append(data_queue.get())

        if data_to_insert:
            try:
                for entry in data_to_insert:
                    node_id = entry['nodeId']
                    sub_type = entry['subType']
                    data_obis_array = entry['dataObis']
                    scalar_obis_array = entry['scalarObis']
                    scalar_array = entry['scalar']
                    debugServerTime = entry['debugServerTime']

                    data_obis = json.dumps(data_obis_array) if data_obis_array else None
                    scalar_obis = json.dumps(scalar_obis_array) if scalar_obis_array else None
                    scalar = json.dumps(scalar_array) if scalar_array else None

                    try:
                        cursor.execute(
                            "INSERT INTO event_data"
                            " (nodeid, subType, DataObis, ScalarObis, Scalar, debugServerTime)"
                            " VALUES (%s, %s, %s, %s, %s, %s)",
                            (node_id, sub_type, data_obis, scalar_obis, scalar, debugServerTime)
                        )

                        connection.commit()

                    except Exception as e:
                        logging.error("Error occurred during bulk insertion: %s", str(e))

            except Exception as e:
                logging.error("Error occurred during bulk insertion: %s", str(e))


def event_consumer():
    consumer = KafkaConsumer(
        bootstrap_servers='10.127.4.99:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    consumer.subscribe(['raw-sensor-data'])

    for messages in consumer:

        profile_type = messages.value['type']
        if profile_type == 'Event_Profile':
            if 'subType' in messages.value:

                print(
                    datetime.now(),
                    'Subtype:', messages.value['subType'],
                    'DataObis:', messages.value['dataObis'],
                    'ScalarObis:', messages.value['scalarObis'],
                    'Scalar:', messages.value['scalar']
                )
                # print(messages.value)
                data_queue.put(messages.value)

            else:
                print('no subType in message')


if __name__ == '__main__':
    bulk_insert_thread = threading.Thread(target=bulk_insert)
    bulk_insert_thread.start()

    try:
        event_consumer()
    except Exception as error:
        logging.error(error)
        logging.error('Stopping the script')

    finally:
        pass

        # cursor.close()
        # connection.close()
