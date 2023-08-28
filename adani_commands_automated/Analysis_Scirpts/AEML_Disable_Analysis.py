import os.path
import psycopg2
import heapq
from datetime import datetime, timedelta


def get_data():
    try:
        connection = psycopg2.connect(
            host="10.127.4.226",
            port="5432",
            user="postgres",
            password="probus@220706",
            database="sensedb"
        )

        cursor = connection.cursor()
        # getting nodes that have given diag but no profile data
        query_1 = """
        
        
        
                    
                    """

        print("executing query 1")
        # checking nodes for billing , packet loss and enable/disable commands
        query_2 = """
        """
        print("executing query 2")

        query_3 = """
                """
        print("executing query 3")

        query_4 = """
                """
        print("executing query 4")

        print("closing database connection")
        cursor.close()
        connection.close()

    except Exception as error:
        print(error)


if __name__ == '__main__':
    get_data()
