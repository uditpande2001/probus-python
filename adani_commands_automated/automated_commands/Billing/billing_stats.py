import psycopg2
import logging

from datetime import datetime, timedelta, date

import csv
import os

req_nodes = []
base_url = 'https://rf-adapter-prod.adanielectricity.com:443'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# making required directories:
today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "Billing_Command_Response"
Response_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(Response_DIRECTORY):
    os.mkdir(Response_DIRECTORY)

RAW_SENSOR_PATH = "Billing_JSONS"
Response_DIRECTORY_2 = os.path.join(ABS_PATH, RAW_SENSOR_PATH)
if not os.path.exists(Response_DIRECTORY_2):
    os.mkdir(Response_DIRECTORY_2)

# -------------------------------------------------------------------------------------------
now = datetime.now()
billing_date_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

priority_nodes = []
with open('priority_nodes_list.csv', 'r', encoding='utf-8-sig') as file:
    csv_reader = csv.reader(file)
    for node in csv_reader:
        priority_nodes.append(int(node[0]))

nodes_tuple = tuple(priority_nodes)


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

        query_1 = f"""
                    
                    SELECT count(DISTINCT node_id) 
                    FROM meter_profile_data mpd 
                    WHERE "type" = 'Billing_Profile'
                    AND date_time = '{billing_date_time}';



           """

        print('executing query 1')
        cursor.execute(query_1)
        results = cursor.fetchall()

        temp = []
        for result in results:
            temp.append(result[0])

        query_1 = f"""

                            SELECT count(DISTINCT node_id) 
                            FROM meter_profile_data mpd 
                            WHERE "type" = 'Billing_Profile'
                            AND date_time = '{billing_date_time}'
                            AND node_id IN {nodes_tuple};



                   """

        print('executing query 2 ')
        cursor.execute(query_1)
        results = cursor.fetchall()
        cursor.close()
        db.close()
        print('closed database connection \n')
        for result in results:
            temp.append(result[0])

        print(
            f" priority nodes (11776) - {temp[1]}  ,  overall - {temp[0]} - -{datetime.now().replace(second=0, microsecond=0)}")



    except Exception as error:
        print(error)


if __name__ == '__main__':
    nodes = get_db_data()
