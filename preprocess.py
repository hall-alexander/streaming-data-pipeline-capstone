import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc, from_json, to_timestamp, window
from pyspark.sql.types import StructType, DoubleType, LongType, StringType, IntegerType
from kafka import KafkaConsumer

# del os.environ["PYSPARK_SUBMIT_ARGS"]
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.amazonaws:aws-java-sdk-bundle:1.11.271,org.apache.hadoop:hadoop-aws:3.1.2 pyspark-shell'

del os.environ["PYSPARK_SUBMIT_ARGS"]
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'

spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('CapstoneApp') \
                    .getOrCreate()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "127.0.0.1:9092").option("subscribe", "position_history").load()

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
        .add('vessel_name', StringType()) \
        .add('vessel_type', StringType()) \
        .add('timestamp_offset_seconds', IntegerType()) \
        .add('true_heading', DoubleType()) \
        .add('rate_of_turn', DoubleType()) \
        .add('repeat_indicator', IntegerType())

df_query = df \
    .selectExpr("cast(value as string)") \
    .select(from_json(col("value").cast("string"), ais_position_history_schema).alias("message"))\
    .select("message.timestamp_utc", "message.mmsi", "message.position", "message.speed_over_ground", "message.course_over_ground", "message.true_heading", "message.rate_of_turn")


# query = df_query.writeStream.outputMode("append").format("console").trigger(processingTime="1 seconds").start()

# There is a Z char to represent zulu time i.e. UTC format. Enclose the Z char with single quotes so the to_timestamp function properly parses the datetime input
df_query_with_timestamp = df_query.withColumn("timestamp_utc", to_timestamp(df_query.timestamp_utc, "yyyy-MM-dd HH:mm:ss'Z'"))


df_windowavg = df_query_with_timestamp.withWatermark("timestamp_utc", "10 minutes").groupBy(
        window(df_query_with_timestamp.timestamp_utc, "5 minutes"),
        df_query_with_timestamp.mmsi).avg("speed_over_ground", "course_over_ground", "true_heading", "rate_of_turn")


df_windowavg_timewindow = df_windowavg.select(
"mmsi",
col("window.start").alias("window_start"),
col("window.end").alias("window_end"),
col("avg(speed_over_ground)").alias("avg_speed_over_ground"),
col("avg(course_over_ground)").alias("avg_course_over_ground"),
col("avg(true_heading)").alias("avg_true_heading"),
col("avg(rate_of_turn)").alias("avg_rate_of_turn")
).orderBy(asc("mmsi"), asc("window_start"))


# valid outputMode options are ["append", "complete", "update"]
# query = df_windowavg_timewindow.writeStream.outputMode("complete").format("console").trigger(processingTime="10 seconds").start()


send_aggregations_to_kafka = df_windowavg_timewindow \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream.outputMode("complete") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("topic", "position_history_kinematic_aggs") \
        .option("checkpointLocation", "checkpoint/send_to_kafka") \
        .start()

send_aggregations_to_kafka.awaitTermination()