from pyspark.sql import SparkSession
from pyspark import SparkConf

from pyspark import SparkConf
conf = SparkConf()
conf.set("spark.jars.packages","mysql:mysql-connector-java:8.0.33")

spark = SparkSession.builder.appName("Chanthru").config(conf=conf).getOrCreate()

mysql_url = "jdbc:mysql://localhost:3306/employees"
mysql_properties = {
    "user": "Chanthru",
    "password": "cluster",
    "driver": "com.mysql.cj.jdbc.Driver"
}

tables = ["employees", "departments", "dept_emp", "dept_manager", "salaries", "titles"]
dfs = {table: spark.read.jdbc(mysql_url, table, properties=mysql_properties) for table in tables}

for table, df in dfs.items():
    print(f"Schema for {table}:")
    df.printSchema()

employees_df = dfs["employees"]
salaries_df = dfs["salaries"]

emp_salaries_df = employees_df.join(salaries_df, "emp_no", "inner")

print("Joined Employees with Salaries:")
emp_salaries_df.show(5)

parquet_path = "output/employees_salaries.parquet"
emp_salaries_df.write.mode("overwrite").parquet(parquet_path)
print(f"Saved Parquet file at {parquet_path}")

df_parquet = spark.read.parquet(parquet_path)
print("Read from Parquet:")
df_parquet.show(5)

df_parquet.write.jdbc(url=mysql_url, table="emp_salaries", mode="overwrite", properties=mysql_properties)
print("Written back to MySQL table 'emp_salaries'.")

spark.stop()