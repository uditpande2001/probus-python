import psycopg2

connection = psycopg2.connect(
    host="10.127.4.226",
    port="5432",
    user="postgres",
    password="probus@220706",
    database="sensedb"
)

cursor = connection.cursor()
query = """SELECT node_id,next_hop
FROM (
  SELECT rd.node_id,rd.next_hop,
         ROW_NUMBER() OVER (PARTITION BY rd.node_id ORDER BY rd.server_time DESC) AS row_num
  FROM rf_diag rd join test t2 on rd.node_id = t2.node_id 
  where rd.server_time between '2023-05-17 00:00:00' and '2023-05-17 17:00:00'
  and rd.end_point like '%253/255'
) t
WHERE row_num = 1;"""

cursor.execute(query)
result = cursor.fetchall()
cursor.close()
connection.close()

nextMap = {}
nextMap.update({r[0]: r[1] for r in result})
childMap = {}
descMap = {}

for n, nextHop in nextMap.items():
    if nextHop < 400000:
        continue

    visited = set()
    current = n

    if nextHop not in childMap:
        childMap[nextHop] = set()
    childMap[nextHop].add(current)

    while nextHop is not None and nextHop >= 400000:
        visited.add(nextHop)

        if nextHop not in descMap:
            descMap[nextHop] = set()
        descMap[nextHop].add(current)

        current = nextHop
        nextHop = nextMap[current] if current in nextMap else None

        if nextHop in visited or nextHop is None:
            break

problematicNodes = set()
for key, value in descMap.items():
    descendants = value
    if len(descendants) > 100 and len(childMap[key]) > 1:
        print("problem:" + key)
        problematicNodes.update(descendants)

print(problematicNodes)
longest_list_size = max(len(lst) for lst in descMap.values())
print(longest_list_size)
