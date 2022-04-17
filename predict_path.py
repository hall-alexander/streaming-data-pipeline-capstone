import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, DoubleType, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window

#del os.environ["PYSPARK_SUBMIT_ARGS"]
os.environ["PYSPARK_PYTHON"] = r"C:\Python39\python.exe"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'

spark = SparkSession.builder \
                    .master('local[2]') \
                    .appName('predict') \
                    .getOrCreate()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "127.0.0.1:9092").option("subscribe", "position_history_kinematic_aggs").load()

features_schema = StructType() \
        .add("mmsi", IntegerType()) \
        .add("timestamp_utc", TimestampType()) \
        .add('speed_over_ground', DoubleType()) \
        .add('course_over_ground', DoubleType()) \
        .add('rate_of_turn', DoubleType()) \
        .add('longitude', DoubleType()) \
        .add('latitude', DoubleType()) \
        .add('cartesian_x', DoubleType()) \
        .add('cartesian_y', DoubleType())

features = df \
    .selectExpr("cast(value as string)") \
    .select(from_json(col("value").cast("string"), features_schema).alias("message"))\
    .select("message.mmsi", 
    "message.timestamp_utc", 
    "message.speed_over_ground", 
    "message.course_over_ground", 
    "message.rate_of_turn",
    "message.longitude",
    "message.latitude",
    "message.cartesian_x",
    "message.cartesian_y")

columns = ["mmsi", "timestamp_utc", "speed_over_ground", "course_over_ground", "rate_of_turn",
    "longitude", "latitude", "cartesian_x", "cartesian_y"]
df_pandas = pd.DataFrame(data=[[-1, "2021-04-05 00:00:00", 5.6, 2.5, 2.5, 101.2, 45.3, 12485.0, 23958352.0, 1]], columns=columns)
global vessel_positions
vessel_positions = spark.createDataFrame(df_pandas)

#features = features.withColumn("timestamp_utc", to_timestamp(features.timestamp_utc, "yyyy-MM-dd HH:mm:ss'Z'"))
#features = features.withColumn("timestamp_utc", to_timestamp(features.timestamp_utc, "yyyy-MM-dd HH:mm:ss"))

def foreach_batch_function(df, epoch_id):
    df.persist()
    global vessel_positions
    print(vessel_positions)
    df1 = vessel_positions.union(df)
    df1.createOrReplaceTempView("current_positions")
    df2 = SparkSession.getActiveSession().sql("""
        with temp as (
            select *, row_number() over (partition by mmsi order by timestamp_utc desc) as row_number 
            from current_positions) 
        select mmsi, timestamp_utc, speed_over_ground, course_over_ground, rate_of_turn, 
            longitude, latitude, cartesian_x, cartesian_y 
        from temp where row_number in (1,2,3)""".strip())
    vessel_positions = df2

features.writeStream.foreachBatch(foreach_batch_function).option("checkpointLocation", r"C:\Users\AlexHall97\Documents\kafka_checkpoints\test").trigger(processingTime="1 minute").start()  


features_out = features.writeStream.outputMode("append") \
    .format("csv") \
    .option("path", r"C:\Users\AlexHall97\Documents\output") \
    .option("checkpointLocation", r"C:\Users\AlexHall97\Documents\kafka_checkpoints\csv") \
    .trigger(processingTime="30 seconds") \
    .start()




def foreach_batch_function(df, epoch_id):
    # Transform and write batchDF
    df.createOrReplaceTempView("batch")
    current = spark.sql("select * from batch")
    df_3 = spark.sql("""with temp as (select *, row_number() over (partition by mmsi order by timestamp_utc desc) as row_number from query q) 
                    select * from temp where row_number in (1,2)""".strip())
    vessel_positions = vessel_positions.alias('vp').join(
        current.alias('c'), 
        ['mmsi'], 
        how='outer'
    ).select("mmsi", "timestamp_utc", "speed_over_ground", "course_over_ground", "rate_of_turn",
    "longitude", "latitude", "cartesian_x", "cartesian_y")
    vessel_positions.createOrReplaceTempView("vessel_positions")
    vessel_positions = spark.sql("""with temp as (select *, row_number() over (partition by mmsi order by timestamp_utc desc) as row_number from vessel_positions vp) 
                    select * from temp where row_number in (1,2,3)""".strip())
    vessel_positions.show()

def foreach_function(row):
    print(row)

features.writeStream.foreach(foreach_function).option("checkpointLocation", r"C:\Users\AlexHall97\Documents\kafka_checkpoints\test").start()  


# features = features.withColumn("window_start_timestamp", to_timestamp(features.window_start, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
# features = features.withColumn("window_end_timestamp", to_timestamp(features.window_end, "yyyy-MM-dd'T'HH:mm:ss'Z'"))

# test = df.writeStream \
#     .option("checkpointLocation", r"C:\Users\LethalCaffeine\Documents\kafka_checkpoints\features") \
#     .toTable("features")

# spark.read.table("features").show()

# test.createOrReplaceTempView("updates")

features.withColumn("")

windowPartition = Window.partitionBy("mmsi").orderBy("window_start")

# spark.sql("select mmsi, window_start, window_end, moving_avg_sog, moving_avg_cog, moving_avg_rot, longitude, latitude, cartesian_x, cartesian_y,  from updates")

features_out = features.writeStream.outputMode("append") \
    .format("csv") \
    .option("path", r"C:\Users\LethalCaffeine\Documents\output") \
    .option("checkpointLocation", r"C:\Users\LethalCaffeine\Documents\output\checkpoint") \
    .trigger(processingTime="30 seconds") \
    .start()




features_out.awaitTermination()