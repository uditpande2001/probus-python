import os.path
import psycopg2
import heapq
from datetime import datetime, timedelta


def get_data():
    try:
        print('connecting to db')
        connection = psycopg2.connect(
            host="10.127.4.226",
            port="5432",
            user="postgres",
            password="probus@220706",
            database="sensedb"
        )

        cursor = connection.cursor()

        print('executing query')

        query_1 = """
                     with temp as (		SELECT date_time, meter_number, node_id, "type", server_time
					    FROM meter_profile_data mpd
					    WHERE server_time BETWEEN '2023-08-02 04:00:00.000' AND '2023-08-02 12:00:00.000'
				    	 AND "type" = 'Instant_Profile'
				    	 limit 1000000)
                          select *
                          from temp 
                        



                    """

        cursor.execute(query_1)
        results = cursor.fetchall()

        for result in results:
            print(result)

        print('closing db connection')
        cursor.close()
        connection.close()


    except Exception as error:
        print(error)


if __name__ == '__main__':
    get_data()
