package consumer


import com.typesafe.config.ConfigFactory
import domain.OrderInfo
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import service.DbService._
import service.OrderService.{addOrderDate, addOrderDateFormatted, processOrdersData}

object ConsumerStreaming {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load
    val inputBootstrapServers = config.getString("input.bootstrap.servers")
    val inputTopic = config.getString("input.topic")
    val pathToFiles = config.getString("pathToFiles")
    val checkpointLocation = config.getString("checkpointLocation")

    implicit val spark = SparkSession.builder
      .appName("ConsumerStreaming")
      .getOrCreate()

    import spark.implicits._


    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", inputBootstrapServers)
      .option("subscribe", inputTopic)
      .load()
      .selectExpr("CAST (value AS STRING)")
      .as[String]
      .map(_.split(","))
      .map(OrderInfo(_))
      .toDF()

    val query =  addOrderDate(df)
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.persist()
        writeToFiles(addOrderDateFormatted(batchDF), pathToFiles)
        writeToPostgres(processOrdersData(batchDF))
        batchDF.unpersist()
        ()
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime("1 minutes"))
      .outputMode("append")
      .start()

    query.awaitTermination()


  }

  def writeToFiles(df: DataFrame, pathToFiles: String)(implicit spark: SparkSession): Unit = {
    df.write
      .option("sep", " ")
      .option("header", "true")
      .partitionBy("order_date")
      .mode(SaveMode.Append)
      .csv(pathToFiles)
  }

  def writeToPostgres(df: DataFrame)(implicit spark: SparkSession): Unit = {
    df.write
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", dbtable + "_current_day")
      .option("user", dbuser)
      .option("password", dbpassword)
      .mode(SaveMode.Append)
      .save()
  }
}
