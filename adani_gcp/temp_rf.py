import psycopg2
import os
from datetime import datetime, timedelta
from master.master_file import list_of_nodes

now = datetime.now()
required_date_time = now - timedelta(hours=4)
date_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
db = None
cursor = None

abs_path = os.path.dirname(__file__)
directory = os.path.join(abs_path, "data")

try:
    print('connecting to database')
    db = psycopg2.connect(
        host='10.127.4.226',
        database='sensedb',
        user='postgres',
        password='probus@220706'

    )
    cursor = db.cursor()
    midnight_query = f"""
                        SELECT node_gw.gw_id, required_meters.node_id FROM
                        (
                            SELECT rd2.gw_id, diag_data.node_id
                            FROM rf_diag rd2
                            RIGHT JOIN
                                (
                                    SELECT node_id , max(server_time) latest_diag_time
                                    FROM rf_diag rd
                                    WHERE server_time >= '{required_date_time}' AND node_id >= 400000
                                    AND end_point like '%253%'
                                    GROUP BY node_id
                                ) diag_data
                            ON rd2.node_id = diag_data.node_id
                            WHERE rd2.server_time = diag_data.latest_diag_time
                        ) node_gw
                        RIGHT JOIN
                            (
                                SELECT mm.meter_number, mm.node_id ,
                                        mid_night.date_time,
                                        mid_night.latest_server_time
                                FROM meter_mapping mm
                                LEFT join
                                            (SELECT meter_list.meter_number,mid_data.node_id,
                                                    mid_data.date_time, meter_list.latest_server_time
                                            FROM
                                            (
                                                SELECT	meter_number,
                                                        node_id ,
                                                        server_time,
                                                        date_time
                                                FROM meter_profile_data mpd
                                        WHERE "type" = 'Midnight_Profile' AND date_time = '{date_time}'
                                            ) mid_data
                                            RIGHT JOIN
                                                    (
                                                        select meter_number , max(server_time) as latest_server_time
                                                        from meter_profile_data mpd
                                                        where "type" ='Midnight_Profile'
                                                        and date_time = '{date_time}'
                                                        and meter_number not like '%:%'
                                                        GROUP BY meter_number
                                                    ) meter_list
                                            ON mid_data.meter_number = meter_list.meter_number
                                            WHERE mid_data.server_time = meter_list.latest_server_time) mid_night
                                    ON 	mm.meter_number = mid_night.meter_number
                                    WHERE mid_night.date_time IS NULL
                            ) required_meters
                        ON node_gw.node_id = required_meters.node_id """
    print("running query")
    cursor.execute(midnight_query)
    results = cursor.fetchall()

    gw_node = {}
    node_list = []

    for key, value in results:
        if int(value) in list_of_nodes:
            if key in gw_node:
                nodes = gw_node[key]
                nodes.append(value)
                gw_node[key] = nodes
            else:
                gw_node[key] = [value]



    # for key, value in gw_node:
    #     if key is not None:
    #         for node in value:
    #             node_list.append(node)
    #     else:
    #         pass

    print(gw_node)



    print("now printing node list")
    print('++++++++++____________________________________________________________++++++++++')


except Exception as error:
    print(error)
finally:
    if cursor is not None:
        print("closed")
        cursor.close()
    if db is not None:
        db.close()
