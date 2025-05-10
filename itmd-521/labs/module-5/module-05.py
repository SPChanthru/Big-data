from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, dayofmonth, hour, avg, max, min, weekofyear, desc, to_date, year
import sys

spark = SparkSession.builder.appName("SF Fire calls analysis").getOrCreate()

if len(sys.argv) != 2:
    print("Usage: spark-submit module-05.py <path_to_sf-fire-calls.csv>")
    sys.exit(1)

file_path = sys.argv[1]
df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

df.printSchema()

df = df.withColumn("CallDate", to_date(col("CallDate"), "yyyy-MM-dd"))


fire_calls_2018 = df.filter(year(col("CallDate")) == 2018)

# 1️ What were all the different types of fire calls in 2018?
print("Different types of fire calls in 2018:")
fire_calls_2018.select("CallType").distinct().show(truncate=False)

# 2️ What months in 2018 had the highest number of fire calls?
print("Fire calls by month in 2018:")
fire_calls_2018.withColumn("Month", month(col("CallDate"))) \
    .groupBy("Month").count() \
    .orderBy(desc("count")) \
    .show(3, False)

# 3️ Which neighborhood in San Francisco generated the most fire calls in 2018?
print("Top neighborhoods with the most fire calls in 2018:")
fire_calls_2018.groupBy("Neighborhood").count().orderBy(desc("count")).show(1, False)

# 4️ Which neighborhoods had the worst response times to fire calls in 2018?
print("Neighborhoods with the worst response times in 2018:")
fire_calls_2018.withColumn("ResponseTime", col("Delay").cast("double")) \
    .groupBy("Neighborhood").agg(avg("ResponseTime").alias("AvgResponseTime")) \
    .orderBy(desc("AvgResponseTime")).show(5, False)

# 5️ Which week in the year 2018 had the most fire calls?
print("Weeks in 2018 with the most fire calls:")
fire_calls_2018.withColumn("Week", weekofyear(col("CallDate"))) \
    .groupBy("Week").count().orderBy(desc("count")).show(1, False)

# 6️ Is there a correlation between neighborhood, zip code, and number of fire calls?
print("Fire calls grouped by Neighborhood & Zip Code:")
fire_calls_2018.groupBy("Neighborhood", "Zipcode").count().orderBy(desc("count")).show(10, False)

# 7️ How can we use Parquet files or SQL tables to store this data and read it back?
parquet_path = "output/fire_calls_2018.parquet"
print(f"Saving the 2018 fire calls dataset to {parquet_path}...")
fire_calls_2018.write.parquet(parquet_path, mode="overwrite")

print("Reading the Parquet file back into a DataFrame:")
df_parquet = spark.read.parquet(parquet_path)
df_parquet.show(5, False)

# Stop Spark Session
spark.stop()
