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
query = """select node_id, server_time, travel_time from rf_diag rd where node_id in ('4837','439210','439697',
'440119','440120','440293','444521','444537','444632','444635','444640','444809','444919','447060','450686',
'467681') and server_time between '2023-06-13 00:00:00' and '2023-06-13 23:59:59' and rd.end_point like '%253/255' 
order by node_id, server_time"""

print("executing query ")
cursor.execute(query)
result = cursor.fetchall()
print("executed")

cursor.close()
connection.close()

previous_node_id = None
previous_next_hop = None
previous_server_time = None

print("calculating")
for row in result:
    node_id, server_time, travel_time = row
    new_server_time = server_time - timedelta(seconds=travel_time)
    if previous_server_time is not None:
        time_difference = new_server_time - previous_server_time
        time_difference_minutes = time_difference.total_seconds() / 60
        if time_difference_minutes > 2:
            print(f"Node ID: {node_id}, Server Time: {server_time}, Time Difference: {time_difference_minutes}")

    # Update the previous values with the current row's values
    previous_node_id = node_id
    previous_server_time = new_server_time

# Close the cursor and connection

