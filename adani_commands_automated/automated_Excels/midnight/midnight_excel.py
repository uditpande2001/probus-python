import os.path
from datetime import date
import psycopg2
from datetime import datetime
import csv

today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = 'midnight_excel_files'
if not os.path.exists(RELATIVE_PATH):
    os.mkdir(RELATIVE_PATH)
file_dir = os.path.join(ABS_PATH, RELATIVE_PATH, today)
if not os.path.exists(file_dir):
    os.mkdir(file_dir)

now = datetime.now()
# diag_server_time = now - timedelta(hours=4)
date_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
server_time_from = now.replace(hour=0, minute=0, second=0, microsecond=0)
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

# billing_date_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
db = None
cursor = None
print(f" Current date time = {now},\n server time >= {server_time_from},\n date_time = {date_time},\n")

try:
    print('connecting to database')
    db = psycopg2.connect(
        host='10.127.4.226',
        database='sensedb',
        user='postgres',
        password='probus@220706'

    )
    cursor = db.cursor()

    try:
        midnight_nodes = []
        with open('node_list.csv', 'r', encoding='utf-8-sig') as file:
            csv_reader = csv.reader(file)
            for node in csv_reader:
                midnight_nodes.append(int(node[0]))

    except Exception as error:
        print(error)


    def get_midnight_excel():
        # nodes_tuple = tuple(node_array)

        midnight_query = f"""	select mm.meter_number,
                COALESCE(mm.node_id, diagnostic_data.node_id) AS node_id,
                instant_data.instant_earliest_time,
                midnight_data.midnight_earliest_time,
                diagnostic_data.gw_id,
                diagnostic_data.n_diag_latest_time,
                diagnostic_data.n_hop_count,
                diagnostic_data.sink_id,
                diagnostic_data.latest_dcu_health,
                diagnostic_data.signal_strength
            from meter_mapping mm
            left join (
                select meter_number,
                    min(server_time) as instant_earliest_time,
                    count(1) as instant_slot_count
                from meter_profile_data mpd
                where "type" = 'Instant_Profile'
                    and server_time >= '{server_time_from}'
                group by meter_number
            ) instant_data on mm.meter_number = instant_data.meter_number
            
             left join (
                select meter_number,
                    min(server_time) as midnight_earliest_time,
                    count(1) as midnight_slot_count
                from meter_profile_data mpd
                where "type" = 'Midnight_Profile'
                     AND date_time = '{date_time}'
                group by meter_number
            ) midnight_data on mm.meter_number = midnight_data.meter_number
                
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
                                            max(server_time) AS n_diag_latest_time,
                                            COUNT(1) AS n_diag_count
                                        FROM rf_diag rd
                                        WHERE node_id >= 400000
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
                                           max(server_time) AS b_diag_latest_time,
                                            COUNT(1) AS b_diag_count
                                        FROM rf_diag rd
                                        WHERE node_id >= 400000
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
                                WHERE health_time >= '{server_time_from}'
                                GROUP BY hub_uuid
                                ) dcu_health_data
                                
                                LEFT join
                                        (
                                        SELECT hub_uuid ,signal_strength,health_time
                                        FROM dcu_health dh 
                                        WHERE health_time >= '{server_time_from}'
                                        
                                        ) dcu_signal 
                                ON dcu_health_data.hub_uuid = dcu_signal.hub_uuid
                                WHERE dcu_health_data.latest_dcu_health = dcu_signal.health_time  
                            ) health_d
                           ON diag_d.gw_id = health_d.hub_uuid
                         )
            ))diagnostic_data on mm.node_id = diagnostic_data.node_id;

                            """

        print("running midnight Excel automate query")
        cursor.execute(midnight_query)
        results = cursor.fetchall()
        db.close()
        print('closed database connection')

        columns = cursor.description
        column_names = []
        for i in columns:
            column_names.append(i[0])

        print("Writing to CSV")
        sheet_name = f'nodes_midnight_data_{date_time.date()}_{timestamp}'
        midnight_sheet = open(file_dir + '\\' + sheet_name + ".csv", 'w', newline='')
        csv_writer = csv.writer(midnight_sheet)
        csv_writer.writerow(column_names)

        for result in results:
            if result[1] in midnight_nodes:
                csv_writer.writerow(result)
        midnight_sheet.close()

        print('file generation completed')

except Exception as error:
    print(error)

# finally:
#     if db is not None:
#         db.close()
#         print("database connection closed")

if __name__ == '__main__':
    # figure out a way to close db connection if it is not closed inside the functions being called
    get_midnight_excel()
