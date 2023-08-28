import psycopg2
import logging

db = None
cursor = None
results = []
array_of_arrays = []

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')


def run_query():
    try:
        logging.info('connecting to database')
        db = psycopg2.connect(
            host='10.127.4.226',
            database='sensedb',
            user='postgres',
            password='probus@220706'

        )
        cursor = db.cursor()
        midnight_query = f"""   SELECT node_id,next_hop
                    FROM (
                      SELECT rd.node_id,rd.next_hop,
                             ROW_NUMBER() OVER (PARTITION BY rd.node_id ORDER BY rd.server_time DESC) AS row_num
                      FROM rf_diag rd join test t2 on rd.node_id = t2.node_id 
                      where rd.server_time between '2023-05-17 16:00:00' and '2023-05-17 17:00:00'
                      and rd.end_point like '%253/255'
                    ) t
                    WHERE row_num = 1;

                            """
        logging.info("hops analyze database query")
        cursor.execute(midnight_query)
        global results
        results = cursor.fetchall()
        db.close()
        logging.info('closed database connection')

        global array_of_arrays

        # Iterate over each tuple in 'results'
        for result in results:
            # Create a new array with the tuple values
            new_array = [result[0], result[1]]

            # Append the new array to 'array_of_arrays'
            array_of_arrays.append(new_array)

        # print(array_of_arrays)
        # print(results)


    except Exception as error:
        print(error)


class Test:
    class Result:
        def __init__(self, nodeId, nextHop):
            self.nodeId = nodeId
            self.nextHop = nextHop

        def getNodeId(self):
            return self.nodeId

        def getNextHop(self):
            return self.nextHop

    def main(self):
        result = []
        nextMap = {}
        for r in result:
            nextMap[r.getNodeId()] = r.getNextHop()

        childMap = {}
        descMap = {}

        for nodeId, nextHop in nextMap.items():
            if nextHop < 400000:
                continue

            visited = set()
            current = nodeId

            if nextHop not in childMap:
                childMap[nextHop] = set()
            childMap[nextHop].add(current)

            while nextHop is not None and nextHop >= 400000:
                visited.add(nextHop)

                if nextHop not in descMap:
                    descMap[nextHop] = set()
                descMap[nextHop].add(current)

                temp = current
                current = nextHop
                nextHop = nextMap.get(temp)

                if nextHop in visited:
                    break

        problematicNodes = set()
        for nextHop, descendants in descMap.items():
            children = childMap.get(nextHop, set())
            if len(descendants) > 100 and len(children) > 1:
                problematicNodes.update(descendants)

        print(problematicNodes)


if __name__ == '__main__':
    run_query()
    print(array_of_arrays)
    test_instance = Test()
    test_instance.main()
