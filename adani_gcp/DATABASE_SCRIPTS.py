import psycopg2
from datetime import datetime, timedelta
from master.master_file import midnight_nodes
import logging

now = datetime.now()
diag_server_time = now - timedelta(hours=4)
date_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
billing_date_time = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
db = None
cursor = None
print(f" Current date time = {now},\n diag_server time >= {diag_server_time},\n date_time = {date_time},\n"
      f" billing_time = {billing_date_time}\n")

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

try:
    logging.info('connecting to database')
    db = psycopg2.connect(
        host='10.127.4.226',
        database='sensedb',
        user='postgres',
        password='probus@220706'

    )
    cursor = db.cursor()


    def get_midnight_query():
        midnight_query = f"""   SELECT max(server_time) , node_id
                                FROM rf_diag rd
                                WHERE server_time >= '{diag_server_time}'
                                AND node_id >=400000
                                AND end_point like '%253%'
                                AND node_id NOT IN	(
                                                        SELECT DISTINCT node_id
                                                        FROM meter_profile_data mpd
                                                        WHERE "type" = 'Midnight_Profile'
                                                        AND date_time= '{date_time}')
                                                        GROUP BY node_id

                            """
        logging.info("running midnight database query")
        cursor.execute(midnight_query)
        results = cursor.fetchall()
        db.close()
        logging.info('closed database connection')
        final_nodes = []
        no_gw = []
        for key, value in results:
            if value in midnight_nodes:
                if key is not None:
                    final_nodes.append(value)
                else:
                    no_gw.append(value)
            else:
                pass

        # print(final_nodes)
        # print(no_gw)

        # print(results)
        # for i in results:
        #     print(i[0])
        #     break

        return final_nodes


    def get_billing_query():
        logging.info("running  billing database query")

        billing_query = f""" SELECT diag_data.gw_id,
                                 billing_not_received.node_id

                                      FROM
                                            (SELECT latest_diag.node_id,
                                            rd.gw_id,
                                            latest_diag.latest_diag_time
                                            FROM rf_diag rd
                                            RIGHT join
                                                        ( 
                                                        SELECT rd.node_id ,
                                                                max(server_time) AS latest_diag_time
                                                        FROM rf_diag rd
                                                        WHERE server_time >= '2023-04-30 20:00:00.000'
                                                         AND node_id >=400000
                                                        AND end_point LIKE '%253%'
                                                        GROUP BY node_id) latest_diag
                                                        ON rd.node_id = latest_diag.node_id
                                                        WHERE rd.server_time = latest_diag.latest_diag_time) diag_data
                                            RIGHT JOIN
                                                        (SELECT mm.meter_number ,
                                                                mm.node_id ,
                                                                billing_given.date_time
                                                        FROM meter_mapping mm
                                                        left join
                                                                (
                                                                SELECT DISTINCT meter_number ,
                                                                node_id ,
                                                                date_time
                                                                FROM meter_profile_data mpd
                                                                WHERE "type" = 'Billing_Profile'
                                                                AND date_time = '2023-05-01 00:00:00.000') billing_given
                                                                ON mm.meter_number = billing_given.meter_number					                            
                                                                ) billing_not_received
                                                        ON diag_data.node_id = billing_not_received.node_id
                                                        WHERE billing_not_received.date_time IS NULL 
                                """
        results = cursor.fetchall()
        logging.info('closed database connection')
        cursor.execute(billing_query)
        final_nodes = []
        no_gw = []
        for key, value in results:
            if value in midnight_nodes:
                if key is not None:
                    final_nodes.append(value)
                elif key is None:
                    no_gw.append(value)
            else:
                pass

        db.close()
        return final_nodes

except Exception as error:
    logging.error(error)

# finally:
#     if db is not None:
#         db.close()
#         print("database connection closed")

if __name__ == '__main__':
    # figure out a way to close db connection if it is not closed inside the functions being called
    # get_billing_query()
    get_midnight_query()
