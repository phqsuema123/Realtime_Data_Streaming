# ✅ import
from cassandra.cluster import Cluster
import pandas as pd

# ✅ connect Cassandra
cluster = Cluster(['localhost'])  # ถ้ารันในเครื่อง
# ถ้าอยู่ใน docker ใช้ 'cassandra'
# cluster = Cluster(['cassandra'])

session = cluster.connect()

# ✅ เลือก keyspace
session.set_keyspace('spark_streams')

# ✅ query data
query = "SELECT * FROM spark_streams.created_users;"
rows = session.execute(query)

# ✅ convert to pandas
data = []
for row in rows:
    data.append(dict(row._asdict()))

df = pd.DataFrame(data)

# ✅ save to CSV
df.to_csv("users_data.csv", index=False)

print("✅ Export CSV สำเร็จ!")