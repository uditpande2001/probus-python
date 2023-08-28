import  time
import requests
import logging
from datetime import datetime, date
import csv
# from automated_commands.master.master_file import ConnectDisconnect_array,


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

if __name__ == '__main__':
    connect()



