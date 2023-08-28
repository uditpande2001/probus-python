import os.path

import psycopg2
import heapq
from datetime import datetime, timedelta

file_name = input(" enter the file name ")


connection = psycopg2.connect(
    host="10.127.4.226",
    port="5432",
    user="postgres",
    password="probus@220706",
    database="sensedb"
)

cursor = connection.cursor()
query = """select hub_uuid,health_time from dcu_health dh where 
            health_time between '2023-08-21 12:00:00' and '2023-08-21 18:30:30'
            order by hub_uuid, health_time"""

print("executing")
cursor.execute(query)
result = cursor.fetchall()
print("executed")

previous_gw_id = None
previous_server_time = None

print("calculating")
with open(file_name+r"_restart_log.txt", 'w+',newline='') as fp:
    for row in result:
        gw_id, server_time = row
        if previous_server_time!=None and gw_id == previous_gw_id:
            time_difference = server_time - previous_server_time
            time_difference_seconds = time_difference.total_seconds()
            if time_difference_seconds > 320 or time_difference_seconds < 280:
                print(f"possible restart gw_id: {gw_id}, Server Time: {server_time}, Time Difference: {time_difference_seconds}")
                fp.write(f"possible restart gw_id: {gw_id}, Server Time: {server_time}, Time Difference: {time_difference_seconds}" + "\n")

        previous_gw_id = gw_id
        previous_server_time = server_time
cursor.close()
connection.close()

