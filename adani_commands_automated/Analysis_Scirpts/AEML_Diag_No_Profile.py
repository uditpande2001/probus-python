import os.path
import psycopg2
from datetime import datetime, timedelta, date

import csv

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = 'DIAG_NO_PROFILE_DATA'
if not os.path.exists(RELATIVE_PATH):
    os.mkdir(RELATIVE_PATH)
file_dir = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(file_dir):
    os.mkdir(file_dir)

current_time = datetime.now()
yesterday_time = current_time - timedelta(days=1)

start_time = yesterday_time.replace(hour=0, minute=0, second=0, microsecond=0)
end_time = yesterday_time.replace(hour=23, minute=59, second=59, microsecond=999)

print(start_time)
print(end_time)


def get_data():
    try:
        connection = psycopg2.connect(
            host="10.127.4.226",
            port="5432",
            user="postgres",
            password="probus@220706",
            database="sensedb"
        )

        cursor = connection.cursor()

        query = f"""
                    WITH t1 AS(
		
                    SELECT rd.node_id , rd.gw_id, req_data.latest_diag
                    FROM rf_diag rd 
                    LEFT join
                            (	SELECT node_id , max(server_time) AS latest_diag
                                FROM rf_diag rd 
                                WHERE server_time BETWEEN '{start_time}' AND '{end_time}'
                                AND node_id >= 400000
                                GROUP BY node_id ) req_data 
                    ON rd.node_id = req_data.node_id
                    WHERE rd.server_time = req_data.latest_diag
        
                ),
                
            t2 AS (
                    SELECT DISTINCT node_id 
                    FROM meter_profile_data mpd 
                    WHERE server_time BETWEEN '{start_time}' AND '{end_time}'
                ),
            t3 AS (
                    SELECT *
                    FROM node n 
                    ),
            t4 AS (
                    SELECT *
                    FROM packet_loss pl 
                    WHERE server_time BETWEEN '{start_time}' AND '{end_time}'
                    AND command_type ='P_READ_BILLING')

            SELECT t1.node_id, t1.gw_id,t1.latest_diag,t3.fw_version
            FROM t1
            LEFT JOIN t2 ON t1.node_id = t2.node_id
            LEFT JOIN t3 ON t1.node_id = t3.node_id
            LEFT JOIN t4 ON t1.node_id = t4.node_id
            WHERE t2 IS NULL AND t4 IS NULL;
                    """

        print("executing query ")

        cursor.execute(query)
        results = cursor.fetchall()

        columns = cursor.description
        column_names = []
        for i in columns:
            column_names.append(i[0])

        sheet_name = f'diag_no_profile_{timestamp}'

        data_sheet = open(file_dir + '\\' + sheet_name + ".csv", 'w', newline='')
        csv_writer = csv.writer(data_sheet)
        csv_writer.writerow(column_names)

        for result in results:
            csv_writer.writerow(result)


        data_sheet.close()
        cursor.close()
        connection.close()

    except Exception as error:
        print(error)


if __name__ == '__main__':
    get_data()
