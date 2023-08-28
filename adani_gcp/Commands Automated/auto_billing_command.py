import logging
import threading
import time
import requests
import psycopg2
from datetime import date
from datetime import datetime, timedelta
from master.master_file import billing_array
from kafka import KafkaConsumer
import json
import csv
import os
# creating different time variables to bes used in the queries that will fetch data from the database
nodes_data = {}
node_to_send_command = []
column_names = []
now = datetime.now()
diag_server_time = now - timedelta(hours=4)
server_time_from = now.replace(hour=0, minute=0, second=0, microsecond=0)
server_time_to = now.replace(hour=23, minute=59, second=59)
midnight_time_from = server_time_to - timedelta(hours=2)
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
billing_date_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
print(billing_date_time)

# creating different variable to store the file path and name where the various csv files will be stored
command_type = ""
today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "Billing_Response"
Response_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(Response_DIRECTORY):
    os.mkdir(Response_DIRECTORY)

base_url = 'https://rf-adapter-prod.adanielectricity.com:443'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def auth():
    try:
        url = base_url + '/auth/login'
        credential = {
            "password": "lAgRGmb8abCVfrBX",
            "userId": "probus"
        }
        response = requests.post(url=url, json=credential)
        logging.info(response.url)

        if response.status_code == 200:
            token = response.text
            logging.info(token)
            return token

        else:
            logging.error(response)
            logging.error(response.text)
            return None
    except requests.exceptions.HTTPError as error:
        logging.error(error)
        return None


res_token = auth()


def get_db_data():
    # get relevant data for given nodes from data_base and store in a dictionary
    try:
        print('connecting to database')
        db = psycopg2.connect(
            host='10.127.4.226',
            database='sensedb',
            user='postgres',
            password='probus@220706'

        )
        cursor = db.cursor()
        # nodes_tuple = tuple(billing_array)

        script = f"""
            select mm.meter_number,
            COALESCE(mm.node_id, diagnostic_data.node_id) AS node_id,
            n.fw_version,
            COALESCE(diagnostic_data.gw_id,last_gw.gw_id) AS last_gw,
            diagnostic_data.sink_id,
            diagnostic_data.n_diag_latest_time,            
            diagnostic_data.n_hop_count,
            diagnostic_data.b_diag_latest_time,
            diagnostic_data.b_hop_count,
            diagnostic_data.latest_dcu_health,
            diagnostic_data.signal_strength,
            init_data.init_latest_time
            from meter_mapping mm
            LEFT join(
                SELECT rd.node_id , rd.gw_id 
                FROM rf_diag rd 
                RIGHT join	(SELECT node_id ,max (server_time) AS latest_time 
                        FROM rf_diag rd 
                        WHERE server_time between  '{server_time_from}' AND '{server_time_to}'
                        AND node_id >= 400000
                        GROUP BY node_id) req_gw
                ON rd.node_id  = req_gw.node_id
                WHERE rd.server_time = req_gw.latest_time)last_gw
                ON mm.node_id = last_gw.node_id
            
            left join (
                select meter_number,
                    max(server_time) as instant_latest_time,
                    count(1) as instant_slot_count
                from meter_profile_data mpd
                where "type" = 'Instant_Profile'
                    and server_time between '{server_time_from}' and '{server_time_to}'
                group by meter_number
            ) instant_data on mm.meter_number = instant_data.meter_number
             left join (
                select meter_number,
                    max(server_time) as midnight_latest_time,
                    count(1) as midnight_slot_count
                from meter_profile_data mpd
                where "type" = 'Midnight_Profile'
                    and server_time between '{midnight_time_from}' and '{server_time_to}' 
                    AND date_time = '{server_time_from}'
                group by meter_number
            ) midnight_data on mm.meter_number = midnight_data.meter_number
            left join (
                select meter_number,
                    max(server_time) as load_latest_time,
                    count(1) as load_slot_count
                from meter_profile_data mpd
                where "type" = 'Load_Profile'
                    and server_time between '{server_time_from}' and '{server_time_to}'
                group by meter_number
            ) load_data on mm.meter_number = load_data.meter_number

            left join (
                select meter_number,
                    max(server_time) as init_latest_time,
                    count(1) as init_count
                from node_init ni
                where server_time between '{server_time_from}' and '{server_time_to}'
                group by meter_number
            ) init_data on mm.meter_number = init_data.meter_number

            full outer join (
                (
                SELECT * 
                FROM (
                        (SELECT COALESCE(nd.node_id, bd.node_id) AS node_id,
                            nd.n_diag_latest_time,
                            nd.n_diag_count,
                            nd.gw_id,
                            nd.sink_id,
                            nd.hop_count AS n_hop_count,
                            bd.b_diag_latest_time,
                            bd.b_diag_count,
                            bd.hop_count AS b_hop_count
                        FROM (
                                SELECT rd2.node_id,rd2.gw_id,rd2.sink_id,
                                    node_diag_data.n_diag_latest_time,
                                    node_diag_data.n_diag_count,
                                    rd2.hop_count
                                FROM rf_diag rd2
                                    RIGHT JOIN (
                                        SELECT node_id,
                                            MAX(server_time) AS n_diag_latest_time,
                                            COUNT(1) AS n_diag_count
                                        FROM rf_diag rd
                                        WHERE node_id >= 400000
                                            AND server_time BETWEEN '{server_time_from}' 
                                            AND '{server_time_to}'
                                            AND end_point LIKE '%253%'
                                        GROUP BY node_id
                                    ) node_diag_data
                                     ON rd2.node_id = node_diag_data.node_id
                                where rd2.server_time = node_diag_data.n_diag_latest_time

                            ) AS nd
                            FULL OUTER JOIN (
                                SELECT rd3.node_id,
                                    boot_diag_data.b_diag_latest_time,
                                    boot_diag_data.b_diag_count,
                                    rd3.hop_count
                                FROM rf_diag rd3
                                    RIGHT JOIN (
                                        SELECT node_id,
                                            MAX(server_time) AS b_diag_latest_time,
                                            COUNT(1) AS b_diag_count
                                        FROM rf_diag rd
                                        WHERE node_id >= 400000
                                            AND server_time BETWEEN '{server_time_from}' 
                                            AND '{server_time_to}'
                                            AND end_point LIKE '%254%'
                                        GROUP BY node_id
                                    ) boot_diag_data ON rd3.node_id = boot_diag_data.node_id
                                where rd3.server_time = boot_diag_data.b_diag_latest_time
                            ) AS bd
                            ON nd.node_id = bd.node_id
                            ) diag_d
                            LEFT JOIN 
                            (        
                                    SELECT dcu_health_data.hub_uuid,
                                    dcu_health_data.latest_dcu_health,
                                    dcu_health_data.dcu_health_count,
                                    dcu_signal.signal_strength
                            FROM  
                                (
                                SELECT hub_uuid , max(health_time) AS latest_dcu_health,
                                        count(hub_uuid) AS dcu_health_count
                                FROM  dcu_health dh
                                WHERE health_time BETWEEN '{server_time_from}' and '{server_time_to}'
                                GROUP BY hub_uuid
                                ) dcu_health_data

                                LEFT join
                                        (
                                        SELECT hub_uuid ,signal_strength,health_time
                                        FROM dcu_health dh 
                                        WHERE health_time BETWEEN  '{server_time_from}' 
                                        and '{server_time_to}'
                                        ) dcu_signal 
                                ON dcu_health_data.hub_uuid = dcu_signal.hub_uuid
                                WHERE dcu_health_data.latest_dcu_health = dcu_signal.health_time  
                            ) health_d
                           ON diag_d.gw_id = health_d.hub_uuid
                         )
            ))diagnostic_data on mm.node_id = diagnostic_data.node_id
            left join node n on mm.node_id = n.node_id ;
        """

        logging.info('executing query')
        cursor.execute(script)
        results = cursor.fetchall()

        logging.info('closed database connection')
        # get column names from the table
        columns = cursor.description

        for i in columns:
            global column_names
            column_names.append(i[0])

        for node in billing_array:
            nodes_data[node] = {
                'meter_number': '',
                'node_id': '',
                'fw_version': '',
                'gw_id': '',
                'sink_id': '',
                'n_diag_latest_time': '',
                'n_hop_count': '',
                'b_diag_latest_time': '',
                'b_hop_count': '',
                'latest_dcu_health': '',
                'signal_strength': '',
                'init_latest_time': '',
                'command_sent_time': '',
                'command_Id': '',
                'accept_status': '',
                'accepted_time': '',
                'accepted_time_stamp': '',
                'execute_status': '',
                'executed_time': '',
                'executed_time_stamp': '',
                'response': ''
            }

        for result in results:
            if result[1] in billing_array:
                # adding data acquired from database for each node
                nodes_data[result[1]]['meter_number'] = result[0]
                nodes_data[result[1]]['node_id'] = result[1]
                nodes_data[result[1]]['fw_version'] = result[2]
                nodes_data[result[1]]['gw_id'] = result[3]
                nodes_data[result[1]]['sink_id'] = result[4]
                nodes_data[result[1]]['n_diag_latest_time'] = result[5]
                nodes_data[result[1]]['n_hop_count'] = result[6]
                nodes_data[result[1]]['b_diag_latest_time'] = result[7]
                nodes_data[result[1]]['b_hop_count'] = result[8]
                nodes_data[result[1]]['latest_dcu_health'] = result[9]
                nodes_data[result[1]]['signal_strength'] = result[10]
                nodes_data[result[1]]['init_latest_time'] = result[11]

                billing_query = f"""
                                        SELECT distinct node_id 
                                        FROM rf_diag rd 
                                        WHERE server_time >='{diag_server_time}'
                                        AND node_id >=400000
                                        AND end_point LIKE '%253%'
                                        AND node_id NOT IN 
                                        (SELECT DISTINCT node_id 
                                        FROM meter_profile_data mpd 
                                        WHERE date_time = '{billing_date_time}' 
                                        AND node_id >=400000
                                        AND "type" ='Billing_Profile') """
                cursor.execute(billing_query)
                results = cursor.fetchall()
                print('executing billing query')

                for result in results:
                    if result[0] in billing_array:
                        # adding required nodes to send command by checking their diagnostics data
                        global node_to_send_command
                        node_to_send_command.append(result[0])


                db.close()

    except Exception as error:
        logging.error(error)


def nodes_batch(node_list):
    url = base_url + '/config/getNodesBatch'

    nodeIds = node_list
    batch_size = {'batchSize': 1}
    head = {
        'Authorization': res_token
    }
    response = requests.post(url, json=nodeIds, params=batch_size, headers=head)
    if response.status_code == 200:
        # logging.info(response.text)
        return response.json()
    else:
        logging.error(response)
        logging.error(response.text)


def billing_command(nodes):
    try:

        batch_array = nodes_batch(nodes)

        # getting number of nodes returned from batch_api
        def count_elements(arr):
            count = 0
            elements_array = []
            for element in arr:
                if isinstance(element, list):
                    nested_count, nested_elements = count_elements(
                        element)  # Recursively count elements in nested array
                    count += nested_count
                    elements_array.extend(nested_elements)
                else:
                    count += 1  # Found a single element
                    elements_array.append(element)  # Add the element to the new array
            return count, elements_array

        num_elements, new_array = count_elements(batch_array)

        print("Total number of nodes:", num_elements)
        # print("New array:", new_array)

        time.sleep(10)
        count = 0

        for array in batch_array:
            for node_id in array:
                count += 1
                print(f" billing command sent to {node_id} count = {count} nodes left = {len(nodes) - count}")
                # adding command send time to nodes_data dictionary
                nodes_data[node_id]['command_sent_time'] = datetime.now()
                command_Id = "%d" % round(time.time())

                nodes_data[node_id]['command_Id'] = command_Id
                # try:
                #     url = base_url + "/command/rfCommand"
                #     headers = {"Authorization": res_token}
                #     sensorCommand = {
                #         'code': 'billing_test',
                #         'commandDestination': "SENSOR",
                #         'commandId': "%d" % round(time.time()),
                #         'commandType': 'P_READ_BILLING',
                #         'debug': True,
                #         'deviceId': node_id,
                #         'hideCommand': True,
                #         'properties': [
                #             {
                #                 'propName': 'P_COUNT',
                #                 'propValue': "1"
                #             }]}
                #     response = requests.post(url, json=sensorCommand, headers=headers)
                #
                #     logging.info(response.url)
                #     if response.status_code == 200:
                #         logging.info(response.text)
                #     else:
                #         logging.error(response)
                #         logging.error(response.text)
                # except requests.exceptions.HTTPError as error:
                #     logging.error(error)

            time.sleep(10)
            if count == num_elements:
                print('Command send to all required nodes, stop the program to generate the excel')
    except Exception as error:
        logging.error(error, exc_info=True)


def cr_consumer():
    consumer = KafkaConsumer(
        bootstrap_servers='10.127.4.99:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))

    )
    consumer.subscribe(['command-response'])

    for message in consumer:
        node_id = message.value['nodeId']
        if int(node_id) in billing_array:
            command_type = str(message.value['commandType'])
            if command_type == 'P_READ_BILLING':
                print(message.value)
                timestamp = message.timestamp
                status = str(message.value['status'])
                # adding data required from consumer to nodes_data dictionary
                if status == "ACCEPTED":
                    nodes_data[node_id]['accept_status'] = status
                    nodes_data[node_id]['accepted_time'] = datetime.now()
                    nodes_data[node_id]['accepted_time_stamp'] = timestamp
                else:
                    nodes_data[node_id]['execute_status'] = status
                    nodes_data[node_id]['executed_time'] = datetime.now()
                    nodes_data[node_id]['executed_time_stamp'] = timestamp
                response = dict(message.value)
                nodes_data[node_id]['response'] = response
                with open("billing_command_JSON.csv", 'w', newline='') as billing_json:
                    csv_writer_json = csv.writer(billing_json)
                    csv_writer_json.writerow(message.value)


if __name__ == '__main__':
    try:
        get_db_data()

        print('RUNNING BILLING COMMAND FUNCTION , WAIT FOR 10 SECONDS')
        command_send_t = threading.Thread(target=billing_command, args=(node_to_send_command,), daemon=True)
        command_send_t.start()
        cr_consumer()
    except Exception as error:
        logging.error(error, exc_info=True)
    finally:

        headers = ['meter_number', 'node_id', 'fw_version', 'gw_id', 'sink_id', 'n_diag_latest_time', 'n_hop_count',
                   'b_diag_latest_time', 'b_hop_count', 'latest_dcu_health', 'signal_strength', 'init_latest_time',
                   'command_sent_time', 'command_Id', 'accept_status', 'accepted_time', 'accepted_time_stamp',
                   'execute_status', 'executed_time', 'executed_time_stamp', 'response'
                   ]

        file_path = os.path.join(Response_DIRECTORY, command_type, today)
        if not os.path.exists(file_path):
            os.mkdir(file_path)
        with open(file_path + '\\' + f"Billing_command_test_file_{timestamp}.csv", 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)

            for node in nodes_data.values():
                writer.writerow([node[key] for key in headers])

        print("Program Stopped File is Generated")
