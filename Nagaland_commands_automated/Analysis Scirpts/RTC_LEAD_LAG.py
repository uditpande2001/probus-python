import psycopg2
from datetime import datetime, timedelta, date
import logging
import csv

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

db = None
cursor = None

current_datetime = datetime.now()


def get_db_data():
    try:
        print('connecting to database', flush=True)
        db = psycopg2.connect(
            host='216.48.180.61',
            database='sensedb',
            user='postgres',
            password='probus@220706'
        )
        cursor = db.cursor()
        midnight_query = f"""					    
                            WITH TEMP AS (SELECT *, 
                                        ROW_NUMBER() OVER(PARTITION BY node_id ORDER BY server_time desc) AS rn
				                FROM meter_profile_data mpd )
                                SELECT *
                                FROM TEMP 
                                WHERE rn = 1 """
        print("running query", flush=True)
        cursor.execute(midnight_query)
        results = cursor.fetchall()
        meters = []
        for result in results:
            sensor_time = result[8]
            tor = result[9]
            server_time = result[2]

            difference_1 = abs((sensor_time - tor).total_seconds() / 3600)
            difference_2 = abs((server_time - tor).total_seconds() / 3600)

            if difference_2 > 12:
                meters.append(result[1])


        print(meters)
        csv_file = 'meters.csv'

        with open(csv_file, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Meter"])  # Write header row
            writer.writerows(zip(meters))

        db.close()


    except Exception as error:
        print(error, flush=True)
    finally:
        if cursor is not None:
            print("database connection closed", flush=True)
            print("\n", flush=True)

            cursor.close()
        if db is not None:
            db.close()


if __name__ == '__main__':
    get_db_data()
