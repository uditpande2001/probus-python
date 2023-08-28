import json
import logging
import os
import ssl
from _datetime import datetime

import paho.mqtt.client as mqtt
import requests

dir_name = os.path.dirname(os.path.realpath(__file__))
logger = logging.getLogger(__name__)


class MqttSensorClientProd:
    def __init__(self, username, password, mqtt_hostname, mqtt_port, gateway_id, mqtt_unsecure):
        client_id = gateway_id + '_dcu_otap_service'
        print(client_id)
        self.mqttc = mqtt.Client()
        self.gw_id = gateway_id
        # self.mqttc.enable_logger()

        # Set ca certificate for ssl
        # mqttc.tls_set(CA_CRT)

        if mqtt_unsecure == 'False':
            self.mqttc.tls_set(tls_version=ssl.PROTOCOL_SSLv23)

        # set username and password
        self.mqttc.username_pw_set(username, password)

        # set host and port then connect
        self.mqttc.connect(mqtt_hostname, mqtt_port)

    def connect(self):
        # Set Logger

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
        # pass
        topic = 'dcu-health-data'
        # topic = '$SYS/#'
        # topic='gw-event/status/#'

        logging.info('subscribing to topic %s', topic)
        self.mqttc.subscribe(topic)

    def on_connect(self, client, userdata, flags, rc):

        if rc == 0:
            self.subscribe_topics()
            logger.info("mqtt connection successfully connect with status code : %s", rc)
        else:
            logger.error("mqtt ping connection failed due to error code : %s", rc)

    def on_message(self, client, obj, msg):
        if "dcu-health-data" in msg.topic:
            payload = str(msg.payload.decode("utf-8", "ignore"))
            try:
                payload_json = json.loads(payload)
                signalStrength = payload_json['signalStrength']
                version = payload_json["version"]
                hubUuid = str(payload_json["hubUuid"])
                read_file = open('DCU_VERSION_v2.0.1.txt', 'r')
                online_gw = []
                # print(hubUuid,version)
                for line in read_file.readlines():
                    line = line.replace("\n", '')
                    online_gw.append(line)
                read_file.close()
                if str(hubUuid + ":" + version) in online_gw:
                    print(str(datetime.now()), "node_id already in file :", hubUuid + ":" + version,signalStrength)
                elif str(version) == 'v2.0.1':
                    write_file = open('DCU_VERSION_v2.0.1.txt', 'a')
                    write_file.write(hubUuid + ":" + version + "\n")
                    write_file.close()
                    print(str(datetime.now()), "online node append", hubUuid + ":" + version,signalStrength)
                else:
                    print(str(datetime.now()), "not updated DCU", hubUuid + ":" + version,signalStrength)
            except json.decoder.JSONDecodeError:
                logging.error("invalid json in command")

    def on_publish(self, client, obj, mid):
        pass

    def on_subscribe(self, client, obj, mid, granted_qos):
        pass

    def on_log(self, client, userdata, level, buf):
        pass

    def send_otap_command(self, basic_auth, otap_type, gw_id, version):
        try:

            url = 'https://rf-adapter-prod.adanielectricity.com:443/config/gwOtap'
            params = {'otapTypes': otap_type,
                      'gwIds': gw_id,
                      'version': version}
            header = {"Authorization": basic_auth}

            response = requests.post(url, params=params, headers=header)

            logging.info(response.url)
            logging.info(response)

            if response.status_code == 200:

                res = response.text
                logging.info(res)
                return res

            else:
                logging.info(response)
                return None

        except requests.exceptions.HTTPError as error:
            logging.error(error)
            return None

    def reboot(self, basic_auth, gw_id):
        try:

            url = 'https://rf-adapter-dev.adanielectricity.com:9999/command/restartGateway'
            # params = {'otapTypes': otap_type,
            #           'gwIds': gw_id,
            #           'version': version}
            #
            params = {'commandId': '12345',
                      'gwId': gw_id
                      }
            header = {"Authorization": basic_auth}

            response = requests.post(url, params=params, headers=header)

            logging.info(response.url)
            logging.info(response)

            if response.status_code == 200:

                res = response.text
                logging.info(res)
                return res

            else:
                logging.info(response)
                return None

        except requests.exceptions.HTTPError as error:
            logging.error(error)
            return None
