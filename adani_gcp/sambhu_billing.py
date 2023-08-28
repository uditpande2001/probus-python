import itertools
from rf_command import billing
import psycopg2
from datetime import datetime, timedelta
import time
from master.master_file import list_of_nodes
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

try:
    logging.info('connecting to database')
    db_conn = psycopg2.connect(
        host='10.127.4.226',
        database='sensedb',
        user='postgres',
        password='probus@220706'

    )

    gw_node_dict = {}
    sql = """
    select diag_node.gw_id, billing_null.node_id from (
    select mm.node_id, mm.meter_number,
    mtr_slot_time.server_time,
    mtr_slot_time.slot_count,
    mtr_slot_time.date_time
    from meter_mapping mm
    left join (
    select midnight_data.meter_number,
    midnight_data.node_id,
    midnight_data.server_time,
    midnight_data.date_time,
    required_mtr_slot.slot_count
    from
    (
    select meter_number , node_id , server_time , date_time from meter_profile_data mpd
    where date_time = '2023-05-01 00:00:00' and "type" = 'Billing_Profile'
    ) midnight_data
    right join
    ( select meter_slot.meter_number, meter_slot.slot_count, meter_slot.last_time from
    (
    select meter_number ,
    count(meter_number) as slot_count ,
    max(server_time) as last_time
    from meter_profile_data mpd
    where "type" = 'Billing_Profile' and date_time = '2023-05-01 00:00:00'
    group by meter_number
    ) meter_slot
    ) required_mtr_slot
    on midnight_data.meter_number = required_mtr_slot.meter_number
    where midnight_data.server_time = required_mtr_slot.last_time
    ) mtr_slot_time
    on mm.meter_number = mtr_slot_time.meter_number
    where mtr_slot_time.slot_count is null
    ) billing_null
    left join
    (
    select rd2.gw_id , rd2.node_id  from rf_diag rd2
    right join
    (
    select node_id , max(server_time) as last_time , count(node_id) as connected_nodes from rf_diag rd
    where server_time >= '2023-05-01 06:00:00' group by node_id
    ) node_time
    on rd2.node_id = node_time.node_id
    where rd2.server_time = node_time.last_time
    ) diag_node
    on billing_null.node_id = diag_node.node_id
    """

    cursor = db_conn.cursor()
    cursor.execute(sql)

    for node_gw in cursor.fetchall():
        if int(node_gw[1]) in list_of_nodes:
            if node_gw[0] is not None:
                if node_gw[0] in gw_node_dict.keys():
                    nodes = gw_node_dict[node_gw[0]]
                    nodes.append(node_gw[1])
                    gw_node_dict[node_gw[0]] = nodes
                else:
                    gw_node_dict[node_gw[0]] = [node_gw[1]]
    db_conn.close()

    nodes_count = 0
    gw_count = 0
    for key in gw_node_dict:
        gw_count += 1
        nodes_count += len(gw_node_dict[key])
        # print(key, len(gw_node_dict[key]), gw_node_dict[key])

    print("Total Gw_id connected is : ", gw_count, "Node Count is : ", nodes_count)
    global_counter = 1
    for values in itertools.zip_longest(*gw_node_dict.values()):
        for node in values:
            if node is not None:
                billing(node)
                print(f"Command sent to : {node} : Counter {global_counter}")
                global_counter += 1
        print("______")
        time.sleep(5)


except Exception as error:
    logging.error(error)
