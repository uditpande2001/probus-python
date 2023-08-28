import time
import random
import ssl
import paho.mqtt.client as mqtt
import json
from gw_master import gwId_update

# MQTT broker details
broker_address = "nms-wirepass-prod.adanielectricity.com"
# broker_port = 8883  # Default port for secure MQTT
# broker_address = "pgcil-iskraemeco-wnt.probussense.com"
broker_port = 8883  # Default port for secure MQTT

# MQTT authentication details
username = "mqttmasteruser"
# adani
password = "ENwQRmAOoCKG2QtAqYWNATqWKINU0Z"
# pgcil-isk
# password = 'fBD6L0jOSbhLL2H2AOlZvWcpdSw1gq'
# MQTT topic
publish_topic = "hubCommandNotification/861261056654714"
subscribe_topic = "hub-command-response"

# MQTT client initialization
client = mqtt.Client()

# Set username and password for authentication
client.username_pw_set(username, password)

# Enable TLS/SSL
client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)


# Callback function for when a connection is established with the MQTT broker
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker")
    # Subscribe to the topic upon successful connection
    client.subscribe(subscribe_topic)


# Callback function for when a message is received
def on_message(client, userdata, message):
    received_data = json.loads(message.payload.decode("utf-8"))
    print(f"Received message: {received_data}")


# Set the callback functions
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker
client.connect(broker_address, broker_port)

# Start the MQTT loop to handle incoming messages
client.loop_start()


# Function to publish data
def publish_data(gw, cmd_type):
    topic = f"hubCommandNotification/{gw}"
    data = {
        "code": "19d69b3b-512f-499d-a0de-e6828d279df0",
        "commandId": 1233,
        "deviceId": "123",
        "commandType": cmd_type,
        "targetGwId": None,
        "commandDestination": "HUB",
        "properties": [{
            "propName": "simType",
            "propValue": "SIM2"
        }, {
            "propName": "switchSim",
            "propValue": True
        }],
        "debug": False,
        "hideCommand": False
    }

    payload = json.dumps(data)  # Convert data to a JSON string

    # Publish the data to the MQTT broker
    client.publish(topic, payload)

    # Print the published data for reference
    print(f"Published data {gw}: {payload}")


# gw_ids = ['861261056660042']

while True:
    for gw in gwId_update:
        publish_data(gw, "HUB_RESTART")
        time.sleep(5)

    break



# command_gw = ["GET_RTC", "ADD_USER" , "SET_LOG_CONFIG", "REMOVE_WIFI", "SET_GW_PASSWORD", "CREATE_REMOVE_FILE", "HUB_RESTART","REMOVE_ADAPTER_FILE"]
