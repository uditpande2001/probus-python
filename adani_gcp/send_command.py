import time
import datetime
import logging
import colorlog
import requests

BASE_URL = 'https://rf-adapter-prod.adanielectricity.com:443'

# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S')


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

def auth():
    try:
        credential = {
            "password": "lAgRGmb8abCVfrBX",
            "userId": "probus"
        }
        url = BASE_URL + '/auth/login'
        response = requests.post(url=url, json=credential)
        logging.info(response.url)
        if response.status_code == 200:
            res_token = response.text
            logging.info(response)
            logging.info(response.text)
            return res_token
        else:
            logging.error(response)

            logging.error(response.text)
            return None
    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


token = auth()


def getHubLogs(gw_id):
    try:
        url = BASE_URL + "/command/getHubLogs"
        headers = {"Authorization": token}
        params = {
            'gwId': gw_id,
            'commandId': "%d" % round(time.time())
        }
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


def hubLogs(gw_id, start_time, end_time):
    try:
        url = BASE_URL + "/command/hubLogs"
        headers = {"Authorization": token}
        params = {
            'gwId': gw_id,
            'startTime': start_time,
            'endTime': end_time
        }
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


list_of_gws = ['866340057573167', '866340057541941', '866340057550488',
               '866340057563614', '866340057566559', '866340057573126', '866340057573365']

global_counter = 1

start_time = "2023-06-25 00:00:00"
end_time = "2023-06-26 00:00:00"

for gw_id in list_of_gws:
    # getHubLogs(gw_id)
    hubLogs(gw_id, start_time, end_time)
    print(f"Command sent to : {gw_id} : Counter {global_counter}")
    global_counter += 1
    time.sleep(5)
