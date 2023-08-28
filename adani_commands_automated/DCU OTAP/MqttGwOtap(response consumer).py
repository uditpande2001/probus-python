import csv
import json
import os
import ssl
from _datetime import datetime
import logging

import paho.mqtt.client as mqtt

dir_name = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(__name__)


class MqttSensorClientProd:
    def __init__(self, username, password, mqtt_hostname, mqtt_port, gateway_id, mqtt_unsecure):
        client_id = gateway_id + '_dcu_otap_service'
        print(client_id)
        self.mqttc = mqtt.Client()
        self.gw_id = gateway_id

        # Set ca certificate for ssl
        # mqttc.tls_set(CA_CRT)

        if mqtt_unsecure == 'False':
            self.mqttc.tls_set(tls_version=ssl.PROTOCOL_SSLv23)

        # set username and password
        self.mqttc.username_pw_set(username, password)

        # set host and port then connect
        self.mqttc.connect(mqtt_hostname, mqtt_port)

    def connect(self):
        # Assign event callbacks
        self.mqttc.on_message = self.on_message
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_subscribe = self.on_subscribe
        self.mqttc.on_log = self.on_log

        # Reconnect
        self.mqttc.loop_forever()

    def publish_data(self, topic, data):
        logging.info("publishing to topic %s data %s", topic, data)
        self.mqttc.publish(topic, data)

    def subscribe_topics(self):
        topic = 'hub-command-response'

        logging.info('subscribing to topic %s', topic)
        self.mqttc.subscribe(topic)

    def on_connect(self, client, userdata, flags, rc):

        if rc == 0:
            self.subscribe_topics()
            logger.info("mqtt connection successfully connect with status code : %s", rc)
        else:
            logger.error("mqtt ping connection failed due to error code : %s", rc)

    def on_message(self, client, obj, msg):
        # if "dcu-health-data" in msg.topic:
        if "hub-command-response" in msg.topic:
            payload = str(msg.payload.decode("utf-8", "ignore"))
            try:
                payload_json = json.loads(payload)
                print(payload_json)
                file = open("dcu_updated_command_response.txt", "a")
                file.write(str(datetime.now()) + "  " + str(payload_json) + "\n")
                file.close()

            except json.decoder.JSONDecodeError:
                logging.error("invalid json in command")

    def on_publish(self, client, obj, mid):
        pass

    def on_subscribe(self, client, obj, mid, granted_qos):
        pass

    def on_log(self, client, userdata, level, buf):
        pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',

                        datefmt='%Y-%m-%d %H:%M:%S')
    mqtt_username = "mqttmasteruser"
    mqtt_password = "sqfjDxtrCN6wNMbp4mq2n2MaZ0m7vnN"
    mqtt_hostname = "216.48.183.143"
    mqtt_port = 1883
    mqtt_unsecure = "True"
    gw_id = "123489"

    mq = MqttSensorClientProd(mqtt_username, mqtt_password, mqtt_hostname, mqtt_port, gw_id,
                              mqtt_unsecure)
    print(mq)
    mq.connect()
