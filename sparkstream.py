# ==============================
# ✅ IMPORT
# ==============================
import logging
import time
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO)


# ==============================
# ✅ CREATE KEYSPACE
# ==============================
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("✅ Keyspace created!")


# ==============================
# ✅ CREATE TABLE
# ==============================
def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
    """)
    print("✅ Table created!")


# ==============================
# ✅ SPARK CONNECTION
# ==============================
def create_spark_connection():
    try:
        spark = SparkSession.builder \
            .appName("SparkDataStreaming") \
            .config("spark.master", "spark://spark-master:7077") \
            .config("spark.jars.packages",
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        print("✅ Spark connected")
        return spark

    except Exception as e:
        print("❌ Spark error:", e)
        return None


# ==============================
# ✅ KAFKA CONNECTION
# ==============================
def connect_to_kafka(spark):
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", "users_created") \
            .option("startingOffsets", "earliest") \
            .load()

        print("✅ Kafka connected")
        return df

    except Exception as e:
        print("❌ Kafka error:", e)
        raise e


# ==============================
# ✅ CASSANDRA CONNECTION (RETRY)
# ==============================
def create_cassandra_connection():
    for i in range(10):
        try:
            cluster = Cluster(["cassandra"])
            session = cluster.connect()
            print("✅ Cassandra connected")
            return session
        except Exception as e:
            print(f"❌ Cassandra retry {i+1}/10:", e)
            time.sleep(5)

    return None


# ==============================
# ✅ PARSE KAFKA JSON
# ==============================
def create_selection_df_from_kafka(df):

    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # ✅ generate UUID
    df = df.withColumn("id", expr("uuid()"))

    return df


# ==============================
# ✅ MAIN
# ==============================
if __name__ == "__main__":

    spark = create_spark_connection()

    if spark:
        kafka_df = connect_to_kafka(spark)
        final_df = create_selection_df_from_kafka(kafka_df)

        session = create_cassandra_connection()

        if session:
            create_keyspace(session)
            create_table(session)

            print("🚀 Starting Streaming...")

            query = final_df.writeStream \
                .format("org.apache.spark.sql.cassandra") \
                .option("checkpointLocation", "/tmp/checkpoint") \
                .option("keyspace", "spark_streams") \
                .option("table", "created_users") \
                .outputMode("append") \
                .start()

            query.awaitTermination()