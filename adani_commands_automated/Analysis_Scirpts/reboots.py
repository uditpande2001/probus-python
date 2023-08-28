import csv
import os
import time
from datetime import datetime

import psycopg2

ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "Test_Sink_reboot"

connection = psycopg2.connect(
    host="10.127.4.226",
    port="5432",
    user="postgres",
    password="probus@220706",
    database="sensedb"
)

cursor = connection.cursor()
query = """select gw_id, count(distinct node_id) as count from rf_diag rd where server_time between 
'2023-08-21 12:00:00' and '2023-08-21 19:00:00' and node_id < 400000 group by gw_id"""

print("executing")
cursor.execute(query)
result = cursor.fetchall()
print("executed")
gw_sink_count = {}
for row in result:
    key = row[0] #gw
    value = row[1]#count
    gw_sink_count[key] = value

query = """select gw_id, node_id, server_time from rf_diag rd where server_time between 
'2023-08-21 12:00:00' and '2023-08-21 19:00:00' and node_id < 400000 and end_point like '%254/255' order by gw_id,
server_time"""

print("executing")
cursor.execute(query)
result = cursor.fetchall()
print("executed")

previous_node_id = None
previous_gw_id = None
previous_server_time = None
counter = 1

time_stamp = round(time.time())
print(time_stamp)

with open(f'test_file_{time_stamp}.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['GW ID', 'Server Time', 'Time Difference'])

print("calculating")
for row in result:
    gw_id, node_id, server_time = row
    if previous_server_time is not None:
        time_difference = server_time - previous_server_time
        # time_difference_minutes = time_difference.total_seconds()/ 60

        time_difference_minutes = time_difference.total_seconds()/ 60
        if time_difference_minutes < 30 and gw_id == previous_gw_id and node_id != previous_node_id:
            counter = counter + 1
            if counter == gw_sink_count.get(gw_id):
                print(f"GW ID: {gw_id}, Server Time: {server_time}, Time Difference: {time_difference_minutes}")
                counter = 1
                with open(f'test_file_{time_stamp}.csv', 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([gw_id, server_time, time_difference_minutes])
        else:
            counter = 1


    previous_node_id = node_id
    previous_server_time = server_time
    previous_gw_id = gw_id

cursor.close()
connection.close()
