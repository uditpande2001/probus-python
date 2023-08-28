import time
import psycopg2
import os
from datetime import datetime, timedelta, date
import logging
import requests
from master.master_file import midnight_nodes

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

now = datetime.now()
date_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
required_diag_time = now - timedelta(hours=4)
yesterday = now - timedelta(days=1)
previous_day_server_time = yesterday.replace(hour=22, minute=0, second=0, microsecond=0)
today_limit = now.replace(hour=23, minute=59, second=59, microsecond=999)

print(f'date_time = {date_time} diag_time = {required_diag_time} ')
print(f'server time from {previous_day_server_time} server_time_to {today_limit}')

db = None
cursor = None

abs_path = os.path.dirname(__file__)
directory = os.path.join(abs_path, "data")

# config for api calls including epoch time creation
base_url = 'https://rf-adapter-prod.adanielectricity.com:443'
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


def midnight(node_id):
    try:
        print(f'command send to {node_id}')
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

    except Exception as error:
        logging.error(error)


def get_db_data():
    try:
        print('connecting to database')
        db = psycopg2.connect(
            host='10.127.4.226',
            database='sensedb',
            user='postgres',
            password='probus@220706'

        )
        cursor = db.cursor()
        midnight_query = f"""   SELECT node_id 
                                FROM rf_diag rd 
                                WHERE server_time >='{required_diag_time}'
                                AND node_id >=400000
                                AND end_point LIKE '%253%'
                                AND node_id NOT IN 
                                (SELECT DISTINCT node_id 
                                FROM meter_profile_data mpd 
                                WHERE date_time = '{date_time}' 
                                AND server_time BETWEEN '{previous_day_server_time}' AND '{today_limit}'
                                AND node_id >=400000
                                AND "type" ='Midnight_Profile'
                                
                                )
                                
     
                           """
        print("running query")
        cursor.execute(midnight_query)
        results = cursor.fetchall()
        total_nodes = []

        for node in results:
            total_nodes.append(node[0])

        # print(node_id)
        return total_nodes

    except Exception as error:
        print(error)
    finally:
        if cursor is not None:
            print(" database connection closed")
            cursor.close()
        if db is not None:
            db.close()


def get_midnight_nodes_data():
    pass


if __name__ == '__main__':
    # these are total nodes which have not given both instant and midnight but have given diag in the
    # last 4 hours
    TOTAL_NODES = get_db_data()
    time.sleep(5)
    required_nodes = []

    for node in midnight_nodes:
        if node in TOTAL_NODES:
            required_nodes.append(node)

    batches = nodes_batch(required_nodes)
    print(batches)

    # executed_file = open(f"D:\\python\\adani_gcp\\Command_Response\\{date.today()}\\P_READ_MIDNIGHT_EXECUTED.csv", 'r')
    # executed_nodes = []
    # for e in executed_file: executed_nodes.append(int(e))
    count = 0
    for node_array in batches:
        size = len(node_array)
        for node in node_array:
            count+=1
            # midnight(node)
            print(count,node)

        time.sleep(10)



