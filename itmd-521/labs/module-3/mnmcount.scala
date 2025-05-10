import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MnMCount")
      .getOrCreate()

    val mnmFile = "/home/vagrant/LearningSparkV2/databricks-datasets/learning-spark-v2/mnm_dataset.csv"

    val mnmDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(mnmFile)

    val countMnMDF = mnmDF
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(col("Total").desc)

    countMnMDF.show(8)

    spark.stop()
  }
}
