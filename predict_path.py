import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, DoubleType, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window

PYTHON_DIR = r"C:\Users\LethalCaffeine\AppData\Local\Programs\Python\Python38\python.exe"
CHECKPOINT_DIR = r"C:\Users\LethalCaffeine\Documents\kafka_checkpoints\features"
OUTPUT_DIR = r"C:\Users\LethalCaffeine\Documents\output\features"

#del os.environ["PYSPARK_SUBMIT_ARGS"]
os.environ["PYSPARK_PYTHON"] = PYTHON_DIR
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'

spark = SparkSession.builder \
                    .master('local[*]') \
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
df_pandas = pd.DataFrame(data=[[-1, "2021-04-05 00:00:00", 5.6, 2.5, 2.5, 101.2, 45.3, 12485.0, 23958352.0]], columns=columns)
global vessel_positions
vessel_positions = spark.createDataFrame(df_pandas)
vessel_positions = vessel_positions.withColumn("timestamp_utc", to_timestamp(vessel_positions.timestamp_utc, "yyyy-MM-dd HH:mm:ss"))
vessel_positions.createGlobalTempView("vessel_positions")

def select_current_vessels(df, epoch_id):
    df.persist()
    print(df.show())
    df.createOrReplaceTempView("current")
    SparkSession.getActiveSession().sql("select * into global_temp.vessel_positions from current")
    df2 = SparkSession.getActiveSession().sql("""
        with temp as (
            select *, row_number() over (partition by mmsi order by timestamp_utc desc) as row_number 
            from vessel_positions),
       select mmsi, timestamp_utc, speed_over_ground, course_over_ground, rate_of_turn, 
            longitude, latitude, cartesian_x, cartesian_y 
        from temp where row_number in (1,2,3)""".strip())
    SparkSession.getActiveSession().sql("delete from global_temp.vessel_positions where mmsi > -4")
    df2.createOrReplaceTempView("recent_positions")
    SparkSession.getActiveSession().sql("select * into global_temp.vessel_positions from recent_positions")
    df3 = SparkSession.getActiveSession().sql("select * from global_temp.vessel_positions")
    df3.write("csv").option("path", OUTPUT_DIR).option("checkpointLocation", CHECKPOINT_DIR).mode("append").save()

# def foreach_batch_function(df, epoch_id):
#     df.persist()
#     df.createOrReplaceTempView("current")
#     spark.sql("select * into vessel_positions from current")
#     current = spark.sql("select * from batch")
#     df1 = vessel_positions.union(df)
#     df1.createOrReplaceTempView("current_positions")
#     df2 = SparkSession.getActiveSession().sql("""
#         with temp as (
#             select *, row_number() over (partition by mmsi order by timestamp_utc desc) as row_number 
#             from current_positions) 
#         select mmsi, timestamp_utc, speed_over_ground, course_over_ground, rate_of_turn, 
#             longitude, latitude, cartesian_x, cartesian_y 
#         from temp where row_number in (1,2,3)""".strip())
#     vessel_positions = df2


# FIXME There seems to be some intricate dependency conflicts happening between java, spark, and the pyspark modules that reference the py4j package to establish a connection to the JVM on the spark cluster
# Can get this to run on laptop but not on desktop. Have to check version numbers later and probably write a dockerfile with a specified java and spark version
# features.writeStream.outputMode("append").foreachBatch(select_current_vessels).start().awaitTermination()

# it appears that whatever sink is specified last in the method chain will be the sink used by the stream
features.writeStream.foreachBatch(select_current_vessels).format("csv").option("path", OUTPUT_DIR).option("checkpointLocation", CHECKPOINT_DIR).trigger(processingTime="30 seconds").start().awaitTerminationOrTimeout(300)
