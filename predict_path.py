import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, DoubleType, StringType, IntegerType

del os.environ["PYSPARK_SUBMIT_ARGS"]
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'

spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('CapstoneApp') \
                    .getOrCreate()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "127.0.0.1:9092").option("subscribe", "position_history_kinematic_aggs").load()

features_schema = StructType() \
        .add("mmsi", IntegerType()) \
        .add("window_start", StringType()) \
        .add("window_end", StringType()) \
        .add('avg_speed_over_ground', DoubleType()) \
        .add('avg_course_over_ground', DoubleType()) \
        .add('avg_true_heading', DoubleType()) \
        .add('avg_rate_of_turn', DoubleType())

features = df \
    .selectExpr("cast(value as string)") \
    .select(from_json(col("value").cast("string"), features_schema).alias("message"))\
    .select("message.mmsi", "message.window_start", "message.window_end", "message.avg_speed_over_ground", "message.avg_course_over_ground", "message.avg_true_heading", "message.avg_rate_of_turn")

features = features.withColumn("window_start_timestamp", to_timestamp(features.window_start, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
features = features.withColumn("window_end_timestamp", to_timestamp(features.window_end, "yyyy-MM-dd'T'HH:mm:ss'Z'"))

query = features.writeStream.outputMode("append").format("console").trigger(processingTime="10 seconds").start()

query.awaitTermination()