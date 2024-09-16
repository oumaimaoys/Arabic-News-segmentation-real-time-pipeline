from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField
import logging
from cassandra.cluster import Cluster
import uuid

# Initialize logging
logging.basicConfig(level=logging.INFO)

def init_spark_session():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark session created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception {e}")

    return s_conn

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created successfully!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.hespress (
            id UUID PRIMARY KEY,
            title TEXT,
            topic TEXT
        );
    """)
    logging.info("Table created successfully!")

def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'hespress') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully")
    except Exception as e:
        logging.error(f"Kafka DataFrame could not be created: {e}")
        return None
    return spark_df

def connect_to_cassandra():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        logging.info("Connected to Cassandra successfully")
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection: {e}")
        return None

def select_df_from_kafka(spark_df):
    schema = StructType([StructField("title", StringType(), True)])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
                  .select(from_json(col('value'), schema).alias('data')) \
                  .select("data.*")
    return sel

def insert_data(session, title, topic):
    try:
        session.execute("""
            INSERT INTO spark_streams.hespress(id, title, topic)
            VALUES (%s, %s, %s)
        """, (uuid.uuid4(), title, topic))
        logging.info(f"Data inserted for {title} with topic {topic}")
    except Exception as e:
        logging.error(f"Could not insert data: {e}")

def process_and_store_predictions(selection_df, model, session):
    predictions = model.transform(selection_df)
    prediction_results = predictions.select("title", "prediction")

    # Loop through predictions and store them in Cassandra
    for row in prediction_results.collect():
        title = row['title']
        topic = str(row['prediction'])  # You might want to map this back to actual topic names
        insert_data(session, title, topic)

if __name__ == "__main__":
    # Initialize Spark connection
    spark_conn = init_spark_session()

    if spark_conn is not None:
        # Load Kafka stream
        spark_df = connect_to_kafka(spark_conn)
        if spark_df:
            selection_df = select_df_from_kafka(spark_df)

            # Connect to Cassandra
            cassandra_session = connect_to_cassandra()

            if cassandra_session is not None:
                create_keyspace(cassandra_session)
                create_table(cassandra_session)

                # Load the saved Spark ML model
                model = PipelineModel.load("/model")

                logging.info("Streaming has started...")

                # Process streaming data and make predictions
                streaming_query = selection_df.writeStream \
                    .foreachBatch(lambda df, _: process_and_store_predictions(df, model, cassandra_session)) \
                    .outputMode("update") \
                    .start()

                streaming_query.awaitTermination()

                # Close Cassandra connection after streaming ends
                cassandra_session.shutdown()
