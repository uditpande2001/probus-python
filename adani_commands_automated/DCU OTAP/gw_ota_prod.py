import logging
import urllib.request

from mqtt_gw_otap_prod import MqttSensorClientProd

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',

                        datefmt='%Y-%m-%d %H:%M:%S')

    mqtt_username = "mqttmasteruser"
    mqtt_password = "ENwQRmAOoCKG2QtAqYWNATqWKINU0Z"
    mqtt_hostname = "nms-wirepass-prod.adanielectricity.com"
    mqtt_port = 8883
    mqtt_unsecure = "False"
    gw_id = "123098"
    mq = MqttSensorClientProd(mqtt_username, mqtt_password, mqtt_hostname, mqtt_port, gw_id,
                              mqtt_unsecure)
    mq.connect()
