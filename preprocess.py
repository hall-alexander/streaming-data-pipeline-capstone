import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc
from pyspark.sql.types import StructType, DoubleType, LongType, StringType, IntegerType
from kafka import KafkaConsumer

del os.environ["PYSPARK_SUBMIT_ARGS"]
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,com.amazonaws:aws-java-sdk-bundle:1.11.271,org.apache.hadoop:hadoop-aws:3.1.2 pyspark-shell'

spark = SparkSession.builder \
                    .master('local[1]') \
                    .appName('CapstoneApp') \
                    .getOrCreate()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "127.0.0.1:9092").option("subscribe", "position_history").load()

df_kafka_string = df.selectExpr("CAST(value AS STRING) as value")

ais_position_history_schema = StructType() \
        .add("timestamp_utc", StringType()) \
        .add("mmsi", IntegerType()) \
        .add("position", StringType()) \
        .add('navigation_status', DoubleType()) \
        .add('speed_over_ground', DoubleType()) \
        .add('course_over_ground', DoubleType()) \
        .add('message_type', IntegerType()) \
        .add('source_identifier', StringType()) \
        .add('position_verified', IntegerType()) \
        .add('position_latency', IntegerType()) \
        .add('raim_flag', IntegerType()) \
        .add('ship_name', StringType()) \
        .add('ship_type', StringType()) \
        .add('timestamp_offset_seconds', IntegerType()) \
        .add('true_heading', DoubleType()) \
        .add('rate_of_turn', DoubleType()) \
        .add('repeat_indicator', IntegerType())

df_windowavg_timewindow = df_windowavg.select(
        "timestamp_utc",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg(speed)").alias("avg_speed"),
        col("avg(accelerometer_x)").alias("avg_accelerometer_x"),
        col("avg(accelerometer_y)").alias("avg_accelerometer_y"),
        col("avg(accelerometer_z)").alias("avg_accelerometer_z")
        ).orderBy(asc("device_id"), asc("window_start"))

query = df_windowavg_timewindow.writeStream.outputMode("complete").format("console").start()

query = df_windowavg_timewindow.writeStream.outputMode("complete").format("kafka").start()
query.awaitTermination()