package common

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestApplication1 {

  // Listen to stream and write to console
  // Use the following command to write to start the steam on port 9001.
  // nc -lk 9001
  var spark = SparkSession.builder()
    .appName("testApplication")
    .master("local[*]")
    .getOrCreate()

  var dataFrame: DataFrame = spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", "9001")
    .load()

  var outputStream = dataFrame.writeStream
    .format("console")
    .outputMode("append")
    .start()

  outputStream.awaitTermination()

  def main(args: Array[String]): Unit = {

  }

}