import sparknlp
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer
from pyspark.sql import DataFrame

spark = sparknlp.start()

#get data
data = pd.read_json("/home/ouyassine/Documents/projects/data_engineering_1/data/training_data.json")
df = spark.createDataFrame(data)

# Shuffle the DataFrame using random sampling
shuffled_df = df.orderBy(F.rand())

# Split the DataFrame into training and testing sets
# Use StringIndexer to convert categorical 'topic' to numeric
indexer = StringIndexer(inputCol="topic", outputCol="topic_index")
indexer_model = indexer.fit(shuffled_df)
indexed_df = indexer_model.transform(shuffled_df)

# Calculate the fraction of each class for stratified sampling
# Here we assume you want an 80/20 split
train_df, test_df = indexed_df.randomSplit([0.8, 0.2], seed=42)

train_df.show(truncate=False)