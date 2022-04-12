import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, DoubleType, StringType, IntegerType
from pyspark.sql.window import Window

del os.environ["PYSPARK_SUBMIT_ARGS"]
os.environ["PYSPARK_PYTHON"] = r"C:\Users\LethalCaffeine\AppData\Local\Programs\Python\Python38\python.exe"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'

spark = SparkSession.builder \
                    .master('local[*]') \
                    .appName('CapstoneApp') \
                    .getOrCreate()

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "127.0.0.1:9092").option("subscribe", "position_history_kinematic_aggs").load()

features = df \
    .selectExpr("cast(value as string)") \
    .select(col("value").cast("string").alias("message"))

features_schema = StructType() \
        .add("mmsi", IntegerType()) \
        .add("window_start", StringType()) \
        .add("window_end", StringType()) \
        .add('moving_avg_sog', DoubleType()) \
        .add('moving_avg_cog', DoubleType()) \
        .add('moving_avg_rot', DoubleType()) \
        .add('longitude', DoubleType()) \
        .add('latitude', DoubleType()) \
        .add('cartesian_x', DoubleType()) \
        .add('cartesian_y', DoubleType())

features = df \
    .selectExpr("cast(value as string)") \
    .select(from_json(col("value").cast("string"), features_schema).alias("message"))\
    .select("message.mmsi", 
    "message.window_start", 
    "message.window_end", 
    "message.moving_avg_sog", 
    "message.moving_avg_cog", 
    "message.moving_avg_rot",
    "message.longitude",
    "message.latitude",
    "message.cartesian_x",
    "message.cartesian_y")

features = features.withColumn("window_start_timestamp", to_timestamp(features.window_start, "yyyy-MM-dd'T'HH:mm:ss'Z'"))
features = features.withColumn("window_end_timestamp", to_timestamp(features.window_end, "yyyy-MM-dd'T'HH:mm:ss'Z'"))

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