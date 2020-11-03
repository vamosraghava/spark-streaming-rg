package common

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration._


object TestApplication1 {

  // Listen to stream and write to console
  // Use the following command to write to start the steam on port 9001.
  // nc -lk 9001
  var spark = SparkSession.builder()
    .appName("testApplication")
    .master("local[*]")
    .getOrCreate()

  var schema = new StructType(Array[StructField](
    StructField("name", StringType),
    StructField("age", IntegerType)
  )
  )

  def readFromSocket(): Unit = {
    val dataFrame: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", "9001")
      .load()

    val outputStream = dataFrame.writeStream
      .format("console")
      .outputMode("append")
      .start()

    outputStream.awaitTermination()
  }

  def readUsingTrigger(): Unit = {
    val dataFrame: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", "9001")
      .load()

    val outputStream = dataFrame.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    outputStream.awaitTermination()
  }

  def readUsingSchema(): Unit = {

    val dataFrame: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", "9001")
      .load()

    val readDF = dataFrame.withColumn("person", from_json(col("value"), schema))
      .drop("value")
      .selectExpr("person.name as name", "person.age as age")

    readDF.printSchema()

    val outputStream = readDF.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start()

    outputStream.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readUsingSchema()
  }
}