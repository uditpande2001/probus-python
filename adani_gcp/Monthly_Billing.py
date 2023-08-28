import time
import requests
import logging
import psycopg2
from master.master_file import enable_array
from datetime import datetime,timedelta



now = datetime.now()
billing_date_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
diag_time = now - timedelta(minutes=45)
print(billing_date_time, diag_time)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

base_url = 'https://rf-adapter-prod.adanielectricity.com/'

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

        billing_query = f"""

                        SELECT DISTINCT node_id
                        FROM rf_diag rd 
                        WHERE server_time >='{diag_time}' 
                        AND node_id >= 400000
                        AND node_id NOT IN 
                                        (	SELECT DISTINCT node_id 
                                            FROM meter_profile_data mpd 
                                            WHERE date_time = ' {billing_date_time}'
                                            AND "type" = 'Billing_Profile');



           """

        print('executing billing query')
        cursor.execute(billing_query)
        results = cursor.fetchall()
        cursor.close()
        db.close()
        print('closed database connection')
        global req_nodes
        for result in results:
            req_nodes.append(result[0])

        return req_nodes

    except Exception as error:
        print(error)

if __name__ == '__main__':

    final_nodes = nodes_batch(enable_array)
    count = 0

    for node_array in final_nodes:
        for node in node_array:
            count += 1
            # billing(node)
            print(f'enable command send to {node} count = {count}')

        time.sleep(10)




