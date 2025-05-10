from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month, dayofmonth, year, desc

import sys

# Initialize Spark Session
spark = SparkSession.builder.appName("Departure_Delays_Analysis").getOrCreate()

if len(sys.argv) != 2:
    print("Usage: spark-submit module-06.py")
    sys.exit(1)

file_path = sys.argv[1]

schema = "`date` STRING, `delay` INT, `distance` INT, `origin` STRING, `destination` STRING"

# Read CSV file into DataFrame
df = spark.read.csv(file_path, schema=schema, header=True)

df.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")

# Convert SQL Queries to DataFrame
# 1. Flights with distance > 1000 miles
df.select("distance", "origin", "destination").filter(col("distance") > 1000).orderBy(desc("distance")).show(10)

# 2. Flights from SFO to ORD with delay > 120
df.select("date", "delay", "origin", "destination").filter(
    (col("delay") > 120) & (col("origin") == "SFO") & (col("destination") == "ORD")
).orderBy(desc("delay")).show(10)

# PART II: Creating tempView and listing columns
df.createOrReplaceTempView("delay_flights_tables")

# Create a tempView 
spark.sql("""
    CREATE OR REPLACE TEMP VIEW ord_march_flights AS
    SELECT * FROM delay_flights_tables
    WHERE origin = 'ORD' AND date LIKE '03%' 
""")

# To show first 5 records from the tempView
spark.sql("SELECT * FROM ord_march_flights LIMIT 5").show()

spark.catalog.listColumns("delay_flights_tables")

# PART III: Write data in different Formats
json_path = "output/departuredelays.json"
parquet_path = "output/departuredelays.parquet"
lz4_json_path = "output/departuredelays_lz4.json"

# to write as JSON
df.write.mode("overwrite").json(json_path)

# Write JSON with LZ4 compression
df.write.mode("overwrite").option("compression", "lz4").json(lz4_json_path)

# write as Parquet
df.write.mode("overwrite").parquet(parquet_path)

# PART IV: To filter the ORD Flights and Write to Parquet
ord_df = df.filter(col("origin") == "ORD")

#To show first 10 records
ord_df.show(10)

# Save ORD departures as Parquet
ord_df.write.mode("overwrite").parquet("output/ord_departuredelays.parquet")



