import time
import requests
import logging
from datetime import datetime, date
from master.master_file import enable_nodes
import csv


base_url = 'https://rf-adapter-prod.adanielectricity.com:443'
current_epoch_time = int(time.time())
from_time_obj = datetime.strptime('00:00:00', '%H:%M:%S')
to_time_obj = datetime.strptime('23:59:59', '%H:%M:%S')
from_time = from_time_obj.time()
to_time = to_time_obj.time()
epoch_from = int(datetime.combine(datetime.today(), from_time).timestamp())
epoch_to = int(datetime.combine(datetime.today(), to_time).timestamp())
def auth():
    try:
        url = base_url + '/auth/login'
        credential = {
            "password": "lAgRGmb8abCVfrBX",
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


if __name__ == "__main__":

    midnight(1234)