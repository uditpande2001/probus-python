import psycopg2
import requests
import logging
import time
import csv

base_url = 'https://rf-adapter-prod.adanielectricity.com:443'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

online_gw = []
gw_node_map = {}


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

def deleteGwObisCacheNode(gw, nodes_array):
    try:
        print(f'clear obis cache command send to {gw}')
        with open('command_send.csv', 'a', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow([gw,nodes_array])

        url = base_url + "/command/deleteGwObisCacheNode"
        headers = {"Authorization": res_token}
        params = {
            'commandId': "%d" % round(time.time()),
            'gwId': f"{gw}_cache_service"
        }

        response = requests.post(url, json=nodes_array, params=params, headers=headers)

        logging.info(response.url)

        if response.status_code == 200:
            logging.info(response)
            logging.info(response.text)
        else:
            logging.error(response)
            logging.error(response.text)

        time.sleep(5)

    except requests.exceptions.HTTPError as error:
        logging.error(error)


def get_db_data(nodes_array):
    try:
        print('connecting to database')
        db = psycopg2.connect(
            host='10.127.4.226',
            database='sensedb',
            user='postgres',
            password='probus@220706'

        )
        cursor = db.cursor()

        gw_query =f""" 
                                            with temp as (
                    select *, row_number() over (partition by hub_uuid order by health_time desc) as rn 
				    from dcu_health dh 
)
                        select hub_uuid from temp
                        where rn = 1
                        and health_time >= '2023-08-28 08:00:00.000'
                    """
        cursor.execute(gw_query)
        results = cursor.fetchall()
        global online_gw
        for gw in results:
            online_gw.append(gw[0])

        print('online_gateways ', online_gw)

        diag_query = f"""
                        SELECT rd2.node_id,rd2.gw_id
		                FROM rf_diag rd2
		                RIGHT JOIN (
		                SELECT node_id,
	                    MAX(server_time) AS n_diag_latest_time	                            
		                FROM rf_diag rd
		                WHERE node_id >= 400000		                         
		                GROUP BY node_id
		                ) node_diag_data
		                ON rd2.node_id = node_diag_data.node_id
		                where rd2.server_time = node_diag_data.n_diag_latest_time
           """

        print('executing  query')
        cursor.execute(diag_query)
        results = cursor.fetchall()
        cursor.close()
        db.close()
        print('closed database connection')

        global gw_node_map

        gw_node_map = {}

        # file = open('cache_cleared_nodes', mode='w', newline='')
        # writer = csv.writer(file)





        print('PERFORMING CHECKS')
        for result in results:
            node_id, gw_id = result

            if gw_id in online_gw:

                if node_id in nodes:

                    with open('diag_nodes_gw''.csv', 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        # time_send = datetime.datetime.now()
                        writer.writerow([gw_id, node_id])
                    if gw_id in gw_node_map:
                        # writer.writerow([node_id])
                        gw_node_map[gw_id].append(node_id)

                    else:
                        # writer.writerow([node_id])
                        gw_node_map[gw_id] = [node_id]

        # print(gw_node_map)
        file.close()


    except Exception as error:
        print(error)

if __name__ == '__main__':

    nodes = []

    with open('nodes_list.csv', 'r', encoding='utf-8-sig') as file:
        csv_reader = csv.reader(file)
        for node in csv_reader:
            nodes.append(int(node[0]))


    get_db_data(nodes)

    count = 0
    for gw, nodes in gw_node_map.items():
        print("Gateway:", gw)
        print("Nodes:", nodes)
        count +=1
        print(count)
        deleteGwObisCacheNode(gw,nodes)


    # final_nodes = get_db_data(nodes)
    # deleteGwObisCacheNode()
    # pass


