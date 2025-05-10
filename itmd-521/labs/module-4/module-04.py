import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder.appName("Divvy_Trips").getOrCreate()

csv_file = sys.argv[1]  


df_inferred = spark.read.format("csv") 
    .option("header", "true") 
    .option("inferSchema", "true") 
    .load(csv_file)

print("Schema using inferSchema:")
df_inferred.printSchema()
print(f"Record count: {df_inferred.count()}")

schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("start_time", StringType(), True),
    StructField("stop_time", StringType(), True),
    StructField("bike_id", IntegerType(), True),
    StructField("trip_duration", IntegerType(), True),
    StructField("from_station_id", IntegerType(), True),
    StructField("from_station_name", StringType(), True),
    StructField("to_station_id", IntegerType(), True),
    StructField("to_station_name", StringType(), True),
    StructField("user_type", StringType(), True),
])

df_programmatic = spark.read.format("csv") 
    .option("header", "true") 
    .schema(schema) 
    .load(csv_file)

print("Schema using StructType:")
df_programmatic.printSchema()
print(f"Record count: {df_programmatic.count()}")

ddl_schema = """
    trip_id INT, start_time STRING, stop_time STRING, bike_id INT, 
    trip_duration INT, from_station_id INT, from_station_name STRING, 
    to_station_id INT, to_station_name STRING, user_type STRING
"""

df_ddl = spark.read.format("csv") 
    .option("header", "true") 
    .schema(ddl_schema) 
    .load(csv_file)

print("Schema using DDL:")
df_ddl.printSchema()
print(f"Record count: {df_ddl.count()}")

spark.stop()
