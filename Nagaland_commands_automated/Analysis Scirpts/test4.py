import psycopg2
import heapq
from datetime import datetime, timedelta

connection = psycopg2.connect(
    host="10.127.4.226",
    port="5432",
    user="postgres",
    password="probus@220706",
    database="sensedb"
)

cursor = connection.cursor()
query = """ select mpd.node_id, server_time, tor 
            from meter_profile_data mpd 
            join test2 t on mpd.node_id = t.node_id 
            where server_time 
            between '2023-07-04 00:00:00' and '2023-07-04 23:59:59' 
            and mpd.type = 'Load_Profile' order by node_id, server_time"""

print("executing")
cursor.execute(query)
result = cursor.fetchall()
print("executed")

previous_node_id = None
previous_next_hop = None
previous_server_time = None

print("calculating")
for row in result:
    node_id, server_time, tor = row
    server_time = tor
    if(previous_server_time!=None):
        time_difference = server_time - previous_server_time
        time_difference_minutes = time_difference.total_seconds() / 60
        if time_difference_minutes > 23:
            print(f"Node ID: {node_id}, Server Time: {server_time}, TOR: {tor}, Time Difference: {time_difference_minutes}")

    # Update the previous values with the current row's values
    previous_node_id = node_id
    previous_server_time = server_time

# Close the cursor and connection
cursor.close()
connection.close()