import logging
from cassandra.cluster import Cluster
from cassandra.io.geventreactor import GeventConnection
from cassandra.io.eventletreactor import EventletConnection
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
import uuid
import asyncore

# Configure logging
logging.basicConfig(level=logging.INFO)

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created successfully!")

def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.vehicle_data (
        id UUID PRIMARY KEY,
        deviceId TEXT,
        timestamp TEXT,
        location TEXT,
        speed FLOAT,
        direction TEXT,
        make TEXT,
        model TEXT,
        year INT,
        fuelType TEXT);
    """)
    logging.info("Vehicle data table created successfully!")

def insert_vehicle_data(session, **kwargs):
    logging.info("Inserting vehicle data...")
    
    vehicle_id = kwargs.get('id', str(uuid.uuid4())) 
    device_id = kwargs.get('deviceId')
    timestamp = kwargs.get('timestamp')
    location = str(kwargs.get('location'))  
    speed = kwargs.get('speed')
    direction = kwargs.get('direction')
    make = kwargs.get('make')
    model = kwargs.get('model')
    year = kwargs.get('year')
    fuel_type = kwargs.get('fuelType')

    try:
        session.execute("""
            INSERT INTO spark_streams.vehicle_data(id, deviceId, timestamp, location, speed, 
                direction, make, model, year, fuelType)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (uuid.UUID(vehicle_id), device_id, timestamp, location, speed,
              direction, make, model, year, fuel_type))
        logging.info(f"Vehicle data inserted for {device_id}")

    except Exception as e:
        logging.error(f'Could not insert vehicle data due to {e}')

def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config("spark.jars", r"C:\Users\houss\Desktop\github-project\smartcity\jars\spark-cassandra-connector_2.13-3.5.1.jar,"
                                   r"C:\Users\houss\Desktop\github-project\smartcity\jars\spark-sql-kafka-0-10_2.13-4.0.0.jar") \
            .config('spark.cassandra.connection.host', "127.0.0.1") \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'vehicle_data') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully")
    except Exception as e:
        logging.warning(f"Kafka DataFrame could not be created because: {e}")

    return spark_df

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'], port=9042)
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("deviceId", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("location", StringType(), False),
        StructField("speed", FloatType(), False),
        StructField("direction", StringType(), False),
        StructField("make", StringType(), False),
        StructField("model", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("fuelType", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
                  .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    return sel

if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka with Spark connection
        spark_df = connect_to_kafka(spark_conn)
        
        selection_df = create_selection_df_from_kafka(spark_df)
        
        # Create Cassandra connection
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            # Use foreachBatch for custom processing or writeStream for direct writes
            def process_batch(batch_df, batch_id):
                for row in batch_df.collect():
                    insert_vehicle_data(session, **row.asDict())

            streaming_query = (selection_df.writeStream.foreachBatch(process_batch)
                               .outputMode("append")
                               .start())

            try:
                streaming_query.awaitTermination()
            except KeyboardInterrupt:
                logging.info("Streaming terminated by user.")
            finally:
                # Close the Cassandra session when done
                session.shutdown()
                spark_conn.stop()
                logging.info("Resources released successfully.")