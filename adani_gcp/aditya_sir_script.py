import csv
import psycopg2
from datetime import datetime, timedelta
import os
import logging
from smtp_mail import send_mail

logging.basicConfig(filename=f'/Total_diag/logs/{(datetime.now())}.log',
                    filemode='w',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelness)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

print(' ENTER REQUIRED TIME INTERVAL IN THE FOLLOWING FORMAT YYYY:MM:DD HH:MM:SS.MS')
test_server_time_from = input(" Enter server time FROM : ")
test_server_time_to = input(" Enter server time TO : ")

print(test_server_time_from)
print(test_server_time_to)
file_name = input(' Enter the name for your file : ')
print(file_name)

# data & time to be passed to the sql script
now = datetime.now()
server_time_from = now - timedelta(hours=2)
server_time_to = now
MIDNIGHT_DATE_TIME = now.replace(hour=0, minute=0, second=0, microsecond=0)
BILLING_DATE_TIME = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

# location where csv file will be made and its name
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = f"Total_node_data\\{datetime.today().date()}"
if not os.path.exists(RELATIVE_PATH):
    os.mkdir(RELATIVE_PATH)
DIAG_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH)
if not os.path.exists(DIAG_DIRECTORY):
    os.mkdir(DIAG_DIRECTORY)

# print(f'from {server_time_from} to {server_time_to}')
db = None
cursor = None
try:
    db = psycopg2.connect(
        host='10.127.4.226',
        database='sensedb',
        user='postgres',
        password='probus@220706'

    )
    logging.info('connecting to database')
    cursor = db.cursor()
    total_diag_query = f"""
    select mm.meter_number,
    COALESCE(mm.node_id, diagnostic_data.node_id) AS node_id,
    n.fw_version,
    diagnostic_data.gw_id,
    diagnostic_data.sink_id,
    diagnostic_data.latest_dcu_health,
    diagnostic_data.dcu_health_count,
    diagnostic_data.signal_strength,
    diagnostic_data.n_diag_latest_time,
    diagnostic_data.n_diag_count,
    diagnostic_data.n_hop_count,
    diagnostic_data.b_diag_latest_time,
    diagnostic_data.b_diag_count,
    diagnostic_data.b_hop_count,
    instant_data.instant_latest_time,
    instant_data.instant_slot_count,
    midnight_data.midnight_latest_time,
    midnight_data.midnight_slot_count,
    load_data.load_latest_time,
    load_data.load_slot_count,
    billing.billing_latest_time,
    billing.billing_slot_count,
    init_data.init_latest_time,
    init_data.init_count
    from meter_mapping mm
        left join (
            select meter_number,
                max(server_time) as instant_latest_time,
                count(1) as instant_slot_count
            from meter_profile_data mpd
            where "type" = 'Instant_Profile'
                and server_time between '{test_server_time_from}' and '{test_server_time_to}'
            group by meter_number
        ) instant_data on mm.meter_number = instant_data.meter_number

     left join (
        select meter_number,
            max(server_time) as midnight_latest_time,
            count(1) as midnight_slot_count
        from meter_profile_data mpd
        where "type" = 'Midnight_Profile'
        and server_time between '{test_server_time_from}' and '{test_server_time_to}'
        AND date_time = '{MIDNIGHT_DATE_TIME}'
        group by meter_number
    ) midnight_data on mm.meter_number = midnight_data.meter_number

    left join (
        select meter_number,
            max(server_time) as load_latest_time,
            count(1) as load_slot_count
        from meter_profile_data mpd
        where "type" = 'Load_Profile'
            and server_time between '{test_server_time_from}' and '{test_server_time_to}'
        group by meter_number
    ) load_data on mm.meter_number = load_data.meter_number

     left join (
        select meter_number,
            max(server_time) as billing_latest_time,
            count(1) as billing_slot_count
        from meter_profile_data mpd
        where "type" = 'Billing_Profile'
            and server_time between '{test_server_time_from}' and '{test_server_time_to}'
            AND date_time ='{BILLING_DATE_TIME}'
        group by meter_number
    ) billing on mm.meter_number = billing.meter_number

    left join (
        select meter_number,
            max(server_time) as init_latest_time,
            count(1) as init_count
        from node_init ni
        where server_time between '{test_server_time_from}' and '{test_server_time_to}'
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
    AND server_time BETWEEN '{test_server_time_from}' AND '{test_server_time_to}'
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
    AND server_time BETWEEN '{test_server_time_from}' AND '{test_server_time_to}'
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
    WHERE health_time BETWEEN '{test_server_time_from}' and '{test_server_time_to}'
    GROUP BY hub_uuid
    ) dcu_health_data
    LEFT join
    (
    SELECT hub_uuid ,signal_strength,health_time
    FROM dcu_health dh
    WHERE health_time BETWEEN  '{test_server_time_from}' and '{test_server_time_to}'
    ) dcu_signal
    ON dcu_health_data.hub_uuid = dcu_signal.hub_uuid
    WHERE dcu_health_data.latest_dcu_health = dcu_signal.health_time
    ) health_d
   ON diag_d.gw_id = health_d.hub_uuid
    )
    ))diagnostic_data on mm.node_id = diagnostic_data.node_id
    left join node n on mm.node_id = n.node_id ;
    """

    logging.info("Running SQL QUERY")
    cursor.execute(total_diag_query)
    results = cursor.fetchall()
    column_names = cursor.description
    field_names = []
    for i in column_names:
        field_names.append(i[0])
    db.close()
    logging.info('database connection closed')

    logging.info("Writing to CSV")
    diag_file = open(DIAG_DIRECTORY + '\\' + file_name + ".csv", 'w', newline='')
    csv_writer = csv.writer(diag_file)
    csv_writer.writerow(field_names)
    for result in results:
        csv_writer.writerow(result)
    diag_file.close()
    # csv_path = f'{DIAG_DIRECTORY}\\{file_name}.csv'
    # send_mail(csv_path, file_name)
    logging.info("Task Completed")
except Exception as error:
    logging.error(error)
finally:
    pass
