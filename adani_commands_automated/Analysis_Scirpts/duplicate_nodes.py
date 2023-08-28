import psycopg2
from datetime import datetime, timedelta

connection = psycopg2.connect(
    host="10.127.4.226",
    port="5432",
    user="postgres",
    password="probus@220706",
    database="sensedb"
)

cursor = connection.cursor()
query = """
            select node_id, gw_id, sink_id
            from rf_diag rd 
            where server_time >= '2023-07-28 10:00:00.0'
            and end_point like '%253/255'
            and node_id in
            (414917,
            600920,
            420397,
            405927,
            428876,
            419081,
            423912,
            423126,
            420002,
            420108,
            410204,
            420209,
            425246,
            412936,
            418668,
            414207,
            408774,
            408313,
            505895,
            446743,
            523064,
            506981,
            900952)order by node_id """


print("executing query ")
cursor.execute(query)
results = cursor.fetchall()
print("executed")

cursor.close()
connection.close()

previous_node_id = None
previous_gw_id = None
previous_sink_id = None
previous_server_time = None

for result in results:
    node_id, gw_id, sink_id = result

    if previous_node_id is not None:
        if node_id == previous_node_id:
            if previous_gw_id != gw_id:
                print(node_id,gw_id,sink_id)

            if previous_sink_id != sink_id:
                print(node_id,gw_id,sink_id)


    previous_node_id = node_id
    previous_gw_id = gw_id
    previous_sink_id = sink_id











