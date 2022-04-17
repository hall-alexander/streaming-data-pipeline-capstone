import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc, from_json, to_timestamp, window, udf, avg, lag, last, first, row_number
from pyspark.sql.types import StructType, DoubleType, LongType, StringType, IntegerType, ArrayType
from kafka import KafkaConsumer
from haversine import haversine, Unit
from pyspark.sql.window import Window
import re
from _util import geodetic_to_geocentric

#del os.environ["PYSPARK_SUBMIT_ARGS"]
os.environ["PYSPARK_PYTHON"] = r"C:\Python39\python.exe"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'

spark = SparkSession.builder \
        .master('local[1]') \
        .appName('preprocess') \
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

def extract_coordinates(position_string):
    coordinates = re.sub('[a-zA-Z()]', '', position_string).split(' ')
    return [float(x) for x in coordinates]

extract_coordinates_udf = udf(lambda x: extract_coordinates(x), ArrayType(DoubleType()))
df_query = df_query.withColumn("geodesic_coordinates", extract_coordinates_udf(col("position")))
df_query = df_query.withColumn("longitude", col("geodesic_coordinates").getItem(0))
df_query = df_query.withColumn("latitude", col("geodesic_coordinates").getItem(1))
df_query = df_query.filter((df_query.latitude > 7) & (df_query.latitude < 23)).filter((df_query.longitude > 105) & (df_query.longitude < 123))

# Transform the geodesic coordinates of the vessels to cartesian
geodesic_to_cartesian_udf = udf(lambda x: geodetic_to_geocentric(x), ArrayType(DoubleType()))
df_query = df_query.withColumn("cartesian_coordinates", geodesic_to_cartesian_udf(col("geodesic_coordinates")))
df_query = df_query.withColumn("cartesian_x", col("cartesian_coordinates").getItem(0))
df_query = df_query.withColumn("cartesian_y", col("cartesian_coordinates").getItem(1))

df_query = df_query.drop('position', 'geodesic_coordinates', 'cartesian_coordinates')

# There is a Z char to represent zulu time i.e. UTC format. Enclose the Z char with single quotes so the to_timestamp function properly parses the datetime input
df_query = df_query.withColumn("timestamp_utc", to_timestamp(df_query.timestamp_utc, "yyyy-MM-dd HH:mm:ss'Z'"))

send_aggregations_to_kafka = df_query \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream.outputMode("update") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("topic", "position_history_kinematic_aggs") \
        .option("checkpointLocation", r"C:\Users\AlexHall97\Documents\kafka_checkpoints\preprocess") \
        .start()

send_aggregations_to_kafka.awaitTermination()

# df_windowavg = df_query.withWatermark("timestamp_utc", "5 minutes").groupBy(
#         window(timeColumn=df_query.timestamp_utc, windowDuration="2 minutes", slideDuration="1 minute"), df_query.mmsi).agg(
#                 avg("speed_over_ground").alias("moving_avg_sog"), 
#                 avg("course_over_ground").alias("moving_avg_cog"), 
#                 avg("rate_of_turn").alias("moving_avg_rot"),
#                 last("longitude").alias("longitude"),
#                 last("latitude").alias("latitude"),
#                 last("cartesian_x").alias("cartesian_x"),
#                 last("cartesian_y").alias("cartesian_y"),
                
#         )

