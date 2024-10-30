from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType
import uuid
from pyspark.ml import PipelineModel

# Initialize Spark session
spark = SparkSession.builder \
    .appName("NewsTopicPrediction") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1,org.apache.hadoop:hadoop-aws:3.2.0") \
    .getOrCreate()

# Kafka topic and S3 bucket details
KAFKA_TOPIC = "yaoum"
KAFKA_BROKER = "broker:29092"
S3_BUCKET = "headlines1234bucket"
S3_OUTPUT_PATH = f"s3a://{S3_BUCKET}/predictions_yaoum/"

# Define schema for Kafka message (including both title and link)
schema = StructType([
    StructField("title", StringType(), True),
    StructField("link", StringType(), True)
])

# Read data from Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Deserialize JSON from Kafka value field
df = df.selectExpr("CAST(value AS STRING)")
df = df.withColumn("value", from_json(col("value"), schema)).select(col("value.*"))

# Generate UUID for each headline
def generate_uuid():
    return str(uuid.uuid4())

uuid_udf = udf(generate_uuid, StringType())

df = df.withColumn("uuid", uuid_udf())

# Load pre-trained model for topic prediction
model = PipelineModel.load("/model")

# Use the model to predict topics using the 'title' field
predictions = model.transform(df.select("title"))

# Add predictions to the original DataFrame with uuid, title, link, and topic
df_with_topics = df.join(predictions.select("title", "prediction"), on="title", how="inner") \
    .withColumnRenamed("prediction", "topic")

# Select relevant columns for final output: uuid, title, link, and predicted topic
df_final = df_with_topics.select("uuid", "title", "link", "topic")

# Function to write the output to S3
def write_to_s3(df, epoch_id):
    df.write \
        .mode("append") \
        .format("csv") \
        .option("header", True) \
        .save(S3_OUTPUT_PATH)

# Write results to S3 in each micro-batch
df_final.writeStream \
    .foreachBatch(write_to_s3) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
