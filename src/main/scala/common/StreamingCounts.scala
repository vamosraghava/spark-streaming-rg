package common

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.concurrent.duration._

object StreamingCounts {

  var spark = SparkSession.builder().master("local[*]")
    .appName("StreamingCounts")
    .getOrCreate();

  def aggFromSocketStream() = {
    val socketStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9001")
      .load()

    val dataFrame: DataFrame = socketStream.selectExpr("COUNT(*) as COUNT")

    dataFrame.writeStream.format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start().awaitTermination()
  }

  def getNumericalAggregations(aggFunction: (Column => Column)*) = {
    val socketStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9001")
      .load()

    val selectExpressions = aggFunction.map(x => x(col("number"))).toArray

    socketStream
      //Cast it to number
      .select(col("value").cast("integer").as("number"))
      .select(selectExpressions: _*)
      .selectExpr("*")
      .writeStream
      .format("console").outputMode("complete")
      .trigger(Trigger.ProcessingTime(2.seconds))
      .start().awaitTermination()
  }


  def main(args: Array[String]): Unit = {
    getNumericalAggregations(sum, avg)
  }
}
