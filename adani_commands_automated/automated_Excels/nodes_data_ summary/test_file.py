import csv
import time

import psycopg2
from datetime import datetime, timedelta, date
import os
import logging

# data & time to be passed to the sql script
now = datetime.now()
server_time_from = now - timedelta(hours=2)
server_time_to = now
MIDNIGHT_DATE_TIME = now.replace(hour=0, minute=0, second=0, microsecond=0)
BILLING_DATE_TIME = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

# location where csv file will be made and its name
today = str(date.today())
ABS_PATH = os.path.dirname(__file__)
RELATIVE_PATH = "Total_node_data"
if not os.path.exists(RELATIVE_PATH):
    os.mkdir(RELATIVE_PATH)
DIAG_DIRECTORY = os.path.join(ABS_PATH, RELATIVE_PATH, today)
if not os.path.exists(DIAG_DIRECTORY):
    os.mkdir(DIAG_DIRECTORY)

db = None
cursor = None

current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)

start_time = datetime(current_hour.year, current_hour.month, current_hour.day, 0)
end_time = current_hour

try :
    db = psycopg2.connect(
                host='10.127.4.226',
                database='sensedb',
                user='postgres',
                password='probus@220706'

            )
    print('connecting to database')
    cursor = db.cursor()
except Exception as error:
    print(error)
count = 0
while start_time < end_time:
    interval_start = start_time
    interval_end = start_time + timedelta(hours=2)

    file_name = f"from_{interval_start.hour}_to_{interval_end.hour}"
    try:

        total_diag_query = f"""
        select mm.meter_number,
        COALESCE(mm.node_id, diagnostic_data.node_id) AS node_id,
        n.fw_version,
        COALESCE(diagnostic_data.gw_id,last_gw.gw_id) AS last_gw,
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
                LEFT join(
                    SELECT rd.node_id , rd.gw_id
                    FROM rf_diag rd
                    RIGHT join	(SELECT node_id ,max (server_time) AS latest_time
                            FROM rf_diag rd
                            WHERE server_time between  '{interval_start}' AND '{interval_end}'
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
                    and server_time between '{interval_start}' and '{interval_end}'
                group by meter_number
            ) instant_data on mm.meter_number = instant_data.meter_number

         left join (
            select meter_number,
                max(server_time) as midnight_latest_time,
                count(1) as midnight_slot_count
            from meter_profile_data mpd
            where "type" = 'Midnight_Profile'
            and server_time between '{interval_start}' and '{interval_end}'
            AND date_time = '{MIDNIGHT_DATE_TIME}'
            group by meter_number
        ) midnight_data on mm.meter_number = midnight_data.meter_number

        left join (
            select meter_number,
                max(server_time) as load_latest_time,
                count(1) as load_slot_count
            from meter_profile_data mpd
            where "type" = 'Load_Profile'
                and server_time between '{interval_start}' and '{interval_end}'
            group by meter_number
        ) load_data on mm.meter_number = load_data.meter_number

         left join (
            select meter_number,
                max(server_time) as billing_latest_time,
                count(1) as billing_slot_count
            from meter_profile_data mpd
            where "type" = 'Billing_Profile'
                and server_time between '{interval_start}' and '{interval_end}'
                AND date_time ='{BILLING_DATE_TIME}'
            group by meter_number
        ) billing on mm.meter_number = billing.meter_number

        left join (
            select meter_number,
                max(server_time) as init_latest_time,
                count(1) as init_count
            from node_init ni
            where server_time between '{interval_start}' and '{interval_end}'
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
        AND server_time BETWEEN '{interval_start}' AND '{interval_end}'
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
        AND server_time BETWEEN '{interval_start}' AND '{interval_end}'
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
        WHERE health_time BETWEEN '{interval_start}' and '{interval_end}'
        GROUP BY hub_uuid
        ) dcu_health_data
        LEFT join
        (
        SELECT hub_uuid ,signal_strength,health_time
        FROM dcu_health dh
        WHERE health_time BETWEEN  '{interval_start}' and '{interval_end}'
        ) dcu_signal
        ON dcu_health_data.hub_uuid = dcu_signal.hub_uuid
        WHERE dcu_health_data.latest_dcu_health = dcu_signal.health_time
        ) health_d
       ON diag_d.gw_id = health_d.hub_uuid
        )
        ))diagnostic_data on mm.node_id = diagnostic_data.node_id
        left join node n on mm.node_id = n.node_id ;
        """

        print("Running SQL QUERY")
        cursor.execute(total_diag_query)
        results = cursor.fetchall()
        column_names = cursor.description
        field_names = []
        for i in column_names:
            field_names.append(i[0])

        count +=1
        print("Writing to CSV")
        print(f"from {interval_start} to {interval_end} ")
        diag_file = open(DIAG_DIRECTORY + '\\' + file_name + ".csv", 'w', newline='')
        csv_writer = csv.writer(diag_file)
        csv_writer.writerow(field_names)
        for result in results:
            csv_writer.writerow(result)
        diag_file.close()
        # csv_path = f'{DIAG_DIRECTORY}\\{file_name}.csv'
        # send_mail(csv_path, file_name)
        print(f"file {count} generation complete")
        print(datetime.now())
        print('\n')
    except Exception as error:
        logging.error(error)
    finally:
        pass

    start_time = interval_end
    time.sleep(5)

db.close()
print('database connection closed')
print('all files generated successfully')
