
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("PythonMnMCount").getOrCreate()

mnm_file = "/home/vagrant/LearningSparkV2/databricks-datasets/learning-spark-v2/mnm_dataset.csv"

mnm_df = spark.read.format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .load(mnm_file)

count_mnm_df = (
    mnm_df
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total"))
    .orderBy(col("Total").desc())
)

count_mnm_df.show(8)

spark.stop()

