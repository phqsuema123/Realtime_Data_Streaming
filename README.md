# 📦 Realtime Data Streaming Pipeline (Airflow + Kafka + Spark + Cassandra)

## 🚀 Overview

โปรเจคนี้เป็น **Realtime Data Pipeline** ที่จำลองการทำงานจริงของ Data Engineer โดยใช้:

* **Airflow** → ดึงข้อมูล API
* **Kafka** → ส่งข้อมูลแบบ streaming
* **Spark Streaming** → ประมวลผลข้อมูล
* **Cassandra** → เก็บข้อมูลแบบ NoSQL

---

## 🏗️ Architecture

```
API → Airflow → Kafka → Spark Streaming → Cassandra
```

---

## ⚙️ Setup & Run (Step-by-Step)

### 1️⃣ Start ทุก Container

```bash
docker compose up -d
```

---

### 2️⃣ เข้า Airflow UI

เปิด:

```
http://localhost:8080
```

Login:

```
Username: admin
Password: admin
```

---

### 3️⃣ Run DAG

* หา DAG ชื่อ: `user_automation`
* กดปุ่ม ▶️ เพื่อ Run

---

### 4️⃣ ตรวจสอบ Kafka (Optional)

เข้า Kafka container:

```bash
docker exec -it broker bash
```

Run consumer:

```bash
kafka-console-consumer \
--bootstrap-server broker:9092 \
--topic users_created \
--from-beginning
```

👉 จะเห็นข้อมูล JSON streaming เข้ามา

---

### 5️⃣ Run Spark Streaming (สำคัญมาก)

เข้า Spark container:

```bash
docker exec -it spark-master bash
```

Run job:

```bash
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
--conf spark.jars.ivy=/tmp/.ivy \
/opt/sparkstream.py
```

---

### 6️⃣ ตรวจสอบข้อมูลใน Cassandra

เข้า Cassandra:

```bash
docker exec -it cassandra cqlsh
```

Query:

```sql
SELECT * FROM spark_streams.created_users;
```

👉 🎉 จะเห็นข้อมูลจาก Airflow → Kafka → Spark → Cassandra ครบ pipeline

---

## 📊 Data Flow Explanation

1. **Airflow DAG**

   * ดึงข้อมูลจาก API (randomuser)
   * ส่งเข้า Kafka topic `users_created`

2. **Kafka**

   * ทำหน้าที่เป็น message broker
   * เก็บ stream ของ user data

3. **Spark Streaming**

   * อ่านข้อมูลจาก Kafka
   * Transform + Clean data
   * เขียนลง Cassandra

4. **Cassandra**

   * เก็บข้อมูลแบบ distributed
   * รองรับ workload สูง

---

## 📁 Project Structure

```
.
├── dags/
│   └── airflow_dag.py
├── spark/
│   └── sparkstream.py
├── docker-compose.yml
└── README.md
```
