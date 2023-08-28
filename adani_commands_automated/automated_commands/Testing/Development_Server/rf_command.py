import time
import requests
import logging
from datetime import datetime, date
from automated_commands.master.master_file import event_array
import csv

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

base_url = ' https://rf-adapter-dev.adanielectricity.com:9999'
current_epoch_time = int(time.time())
from_time_obj = datetime.strptime('00:00:00', '%H:%M:%S')
to_time_obj = datetime.strptime('23:59:59', '%H:%M:%S')
from_time = from_time_obj.time()
to_time = to_time_obj.time()
epoch_from = int(datetime.combine(datetime.today(), from_time).timestamp())
epoch_to = int(datetime.combine(datetime.today(), to_time).timestamp())
print('from epoch ', epoch_from)
print('to epoch ', epoch_to)
print(f"{date.today()}")


def auth():
    try:
        url = base_url + '/auth/login'
        credential = {
            "password": "kBgRGmb9abCVfrAT",
            "userId": "probus"
        }
        response = requests.post(url=url, json=credential)
        logging.info(response.url)

        if response.status_code == 200:
            token = response.text
            logging.info(token)
            return token

        else:
            logging.error(response)
            logging.error(response.text)
            return None
    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


res_token = auth()


def midnight(node_id):
    url = base_url + '/command/rfCommand'

    sensorCommand = {'code': "midnight_test",
                     'commandDestination': "SENSOR",
                     'commandId': "%d" % round(time.time()),
                     'commandType': "P_READ_MIDNIGHT",
                     'debug': True,
                     'deviceId': node_id,
                     'hideCommand': True,
                     'properties': [
                         {
                             "propName": "P_FROM",
                             "propValue": f'{epoch_from}',
                         },
                         {
                             "propName": "P_TO",
                             "propValue": f'{epoch_to}'
                         }
                     ]
                     }
    head = {'Authorization': res_token}
    response = requests.post(url=url, json=sensorCommand, headers=head)
    if response.status_code == 200:
        logging.info(response)
        logging.info(response.text)
    else:
        logging.error(response)
        logging.error(response.text)


def billing(node_id):
    try:
        url = base_url + "/command/rfCommand"
        headers = {"Authorization": res_token}
        sensorCommand = {
            'code': 'billing_test',
            'commandDestination': "SENSOR",
            'commandId': "%d" % round(time.time()),
            'commandType': 'P_READ_BILLING',
            'debug': True,
            'deviceId': node_id,
            'hideCommand': True,
            'properties': [
                {
                    'propName': 'P_COUNT',
                    'propValue': "1"
                }]}
        response = requests.post(url, json=sensorCommand, headers=headers)

        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def list_command(node_id):
    try:
        url = base_url + "/command/listCommands"
        headers = {"Authorization": res_token}
        params = {
            'nodeId': node_id,
            'commandId': "%d" % round(time.time())
        }
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)


def load(node_id):
    url = base_url + '/command/rfCommand'

    sensorCommand = {'code': "load_test",
                     'commandDestination': "SENSOR",
                     'commandId': "%d" % round(time.time()),
                     'commandType': "P_READ_LOAD",
                     'debug': True,
                     'deviceId': node_id,
                     'hideCommand': True,
                     'properties': [
                         {
                             "propName": "P_FROM",
                             "propValue": 1684348200,
                         },
                         {
                             "propName": "P_TO",
                             "propValue": 1684434600
                         }
                     ]
                     }
    head = {'Authorization': res_token}
    response = requests.post(url=url, json=sensorCommand, headers=head)
    if response.status_code == 200:
        logging.info(response)
        logging.info(response.text)
    else:
        logging.error(response)
        logging.error(response.text)


def nodes_batch(node_list):
    url = base_url + '/config/getNodesBatch'

    nodeIds = node_list
    batch_size = {'batchSize': 1}
    head = {
        'Authorization': res_token
    }
    response = requests.post(url, json=nodeIds, params=batch_size, headers=head)
    if response.status_code == 200:
        # logging.info(response.text)
        return response.json()
    else:
        logging.error(response)
        logging.error(response.text)


def enable_all(node_id):
    try:
        url = base_url + "/command/enableAll"
        header = {"Authorization": res_token}
        params = {
            'nodeId': node_id,
            'commandId': "%d" % round(time.time()),
            'broadcast': False

        }
        response = requests.post(url=url, params=params, headers=header)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)

    except requests.exceptions.HTTPError as error:
        logging.error(error)


def connect(node_id):
    try:
        url = base_url + "/command/connectDisconnect"
        header = {"Authorization": res_token}
        params = {
            'nodeId': node_id,
            'commandId': "%d" % round(time.time()),
            'state': 'CONNECTED',
            'mode': 'MODE_NONE',
            'meterMaker': 'GENUS',

        }
        response = requests.post(url=url, params=params, headers=header)
        # logging.info(response.url)
        if response.status_code == 200:
            pass
            # logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)

    except requests.exceptions.HTTPError as error:
        logging.error(error)


def disconnect(node_id):
    try:
        url = base_url + "/command/connectDisconnect"
        header = {"Authorization": res_token}
        params = {
            'nodeId': node_id,
            'commandId': "%d" % round(time.time()),
            'state': 'DISCONNECTED',
            'mode': 'MODE_NONE',
            'meterMaker': 'GENUS',

        }
        response = requests.post(url=url, params=params, headers=header)
        # logging.info(response.url)
        if response.status_code == 200:
            pass

            # logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)

    except requests.exceptions.HTTPError as error:
        logging.error(error)


def getConnectState(node_id):
    try:
        url = base_url + "/command/getConnectState"
        header = {"Authorization": res_token}
        params = {
            'nodeId': node_id,
            'commandId': "%d" % round(time.time()),
            'meterMaker': 'GENUS',

        }
        response = requests.post(url=url, params=params, headers=header)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)

    except requests.exceptions.HTTPError as error:
        logging.error(error)


def restartGateway(gw_id):
    try:
        url = base_url + "/command/restartGateway"
        header = {"Authorization": res_token}
        params = {

            'commandId': "%d" % round(time.time()),
            'gwId': gw_id,
        }
        response = requests.post(url=url, params=params, headers=header)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)

    except requests.exceptions.HTTPError as error:
        logging.error(error)


def restartNode(node_id):
    try:
        url = base_url + "/command/restartNode"
        headers = {"Authorization": res_token}
        params = {'nodeId': node_id}
        response = requests.post(url, params=params, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)
    except requests.exceptions.HTTPError as error:
        logging.error(error)

def event(node_id):
    try:
        cmd_id = int(time.time())
        volt_properties = {
            'P_TYPE': 0,
            'P_COUNT': 5
        }
        url = base_url + "/command/rfCommand"
        headers = {"Authorization": res_token}
        sensorCommand = {
            'code': 'event_test',
            'commandDestination': "SENSOR",
            'commandId': cmd_id,
            'commandType': 'P_READ_EVENT',
            'debug': True,
            'deviceId': node_id,
            "hideCommand": True,
            'properties': [
                {'propName': 'P_TYPE', 'propValue': volt_properties['P_TYPE']},
                {'propName': 'P_COUNT', 'propValue': volt_properties['P_COUNT']}
            ]}
        response = requests.post(url, json=sensorCommand, headers=headers)
        logging.info(response.url)
        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)

    except requests.exceptions.HTTPError as error:
        logging.error(error)

if __name__ == '__main__':

    count = 0

    for node in event_array:
        count += 1
        event(node)
        print(node, datetime.now(), count)
        time.sleep(15)

    # result_nodes = nodes_batch(connect_nodes)

    # executed_file = open(f"D:\\python\\adani_gcp\\Command_Response\\{date.today()}\\P_READ_BILLING_EXECUTED.csv", 'r')
    # executed_nodes = []
    # for e in executed_file: executed_nodes.append(int(e))

    # count = 0
    # for node_array in result_nodes:
    #     for node in node_array:
    #         # if node not in executed_nodes:
    #             count += 1
    #
    #             print(count)
    #             # restartNode(node)
    #             connect(node)
    #             print('billing command send to ', node)
    #
    #     time.sleep(10)

    # file = open('List_command_send_time.csv', 'a', newline='')
    # writer = csv.writer(file)
    # writer.writerow(["NodeId", "Sent_Time"])
    # for nodes_array in result_nodes:
    # for node_array in result_nodes:
    #     for node in node_array:
    #         list_command(node)
    #         current_time = datetime.now()
    #         writer.writerow([node, str(current_time)])
    #         print(f"Command send to {node} : Count = {count}")
    #         count += 1
    #
    #     time.sleep(5)
    # file.close()
