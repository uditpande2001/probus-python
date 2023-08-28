import os.path
import psycopg2
from datetime import datetime, timedelta, date

import csv

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = 'DISABLE_ANALYSIS'
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

        query1 = f"""
                            WITH t1 AS (
                             SELECT DISTINCT node_id 
                             FROM command_response cr 
                             WHERE server_time BETWEEN '2023-07-03 00:00:00.000' AND '2023-07-03 23:59:59.999'
                             AND command_type IN ('ENABLE_ALL', 'DISABLE_ALL')
                            ),
                    t2 AS  (
                            SELECT *
                            FROM packet_loss pl 
                            WHERE server_time BETWEEN '2023-07-03 00:00:00.000' AND '2023-07-03 23:59:59.999'
                            AND command_type ='P_READ_BILLING'
                            ),
                    T3 AS (
                            SELECT *
                            FROM meter_profile_data mpd 
                            WHERE server_time BETWEEN '2023-07-03 00:00:00.000' AND '2023-07-03 23:59:59.999'
                            AND "type" = 'Billing_Profile'
                            ),
                    t4 AS (
                            SELECT *
                            FROM node n 
                            )
                
                SELECT t1.node_id, t4.fw_version
                FROM t1
                LEFT JOIN t2 ON t1.node_id = t2.node_id 
                LEFT JOIN t3 ON t1.node_id = t3.node_id
                LEFT JOIN t4 ON t4.node_id = t1.node_id
                WHERE t2 IS NULL AND t3 IS NULL ;
                    

                    """

        print("executing query1 ")

        query2 = f"""
                    SELECT enable_count.node_id,enable_count.Enable_all_count,
                     enable_count.latest_time AS enable_latest_time,
		            enable_data.command_id AS enable_command_id
                    FROM 
                    (SELECT node_id , count(1) AS Enable_all_count ,max(server_time) AS latest_time  
                    FROM command_response cr 
                    WHERE server_time BETWEEN '2023-07-03 00:00:00.000' AND '2023-07-03 23:59:59.999'
                    AND command_type = 'ENABLE_ALL' AND status ='ACCEPTED'
                    GROUP BY node_id) enable_count
                    LEFT JOIN 
                            (SELECT *
                                FROM command_response cr 
                                WHERE server_time BETWEEN 
                                '2023-07-03 00:00:00.000' AND '2023-07-03 23:59:59.999') enable_data
                    ON enable_count.node_id = enable_data.node_id
                    WHERE enable_count.latest_time = enable_data.server_time
                    ORDER BY node_id ;

                    """

        print("executing query2 ")

        cursor.execute(query1)
        results1 = cursor.fetchall()

        cursor.execute(query2)
        results2 = cursor.fetchall()

        joined_result = [(r1,r2) for r1 in results1 for r2 in results2 if r1[0] == r2[0]]

        # columns = cursor.description
        # column_names = []
        # for i in columns:
        #     column_names.append(i[0])

        sheet_name = f'diag_no_profile_{timestamp}_temp'

        data_sheet = open(file_dir + '\\' + sheet_name + ".csv", 'w', newline='')
        csv_writer = csv.writer(data_sheet)

        for row  in joined_result:
            csv_writer.writerow(row)

        data_sheet.close()
        cursor.close()
        connection.close()

    except Exception as error:
        print(error)


if __name__ == '__main__':
    get_data()
