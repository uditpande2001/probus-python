import psycopg2
import os
from datetime import datetime, timedelta
import csv

db = None
cursor = None

try:

    abs_path = os.path.dirname(__file__)
    directory = os.path.join(abs_path, "nodes_log")
    if not os.path.exists(directory):
        os.mkdir(directory)

    print('connecting to database')
    db = psycopg2.connect(
        host='10.127.4.226',
        database='sensedb',
        user='postgres',
        password='probus@220706'

    )
    file_name = input(' Enter the name for log file : ')
    print(file_name)
    cursor = db.cursor()
    query = f"""
                SELECT rl.node_id ,rl.hex,rl.server_time AS log_time ,req_nodes.instant_slots	
                FROM rf_log rl 
                RIGHT JOIN	(
                            SELECT node_id , max(server_time)as latest_time , count(node_id) AS instant_slots 
                            FROM meter_profile_data mpd 
                            WHERE server_time BETWEEN '2023-05-10 00:00:00.000' AND '2023-05-10 23:59:59.999'
                            AND "type" = 'Instant_Profile' AND node_id >= 400000
                            GROUP BY node_id ) req_nodes
                ON rl.node_id = req_nodes.node_id
                WHERE req_nodes.instant_slots <5
                AND rl.server_time BETWEEN '2023-05-10 00:00:00.000' AND '2023-05-10 23:59:59.999'

                        """

    print("running query")
    cursor.execute(query)
    results = cursor.fetchall()
    # print(results)

    columns = cursor.description
    column_names = []
    for i in columns:
        column_names.append(i[0])

    print("Writing to CSV")
    sheet_name = f'{file_name}'
    log_sheet = open('nodes_log' + '\\' + sheet_name + ".csv", 'w', newline='')
    csv_writer = csv.writer(log_sheet)
    csv_writer.writerow(column_names)
    for result in results:
        csv_writer.writerow(result)

    csv_writer.writerow([])
    log_sheet.close()

except Exception as error:
    print(error)
finally:
    if cursor is not None:
        print("closed")
        cursor.close()
    if db is not None:
        db.close()
