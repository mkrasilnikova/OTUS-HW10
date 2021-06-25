package consumer


import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import service.DbService._
import service.OrderService.{addOrderDate, processOrdersData}

object ConsumerBatch {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load
    val pathToFiles = config.getString("pathToFiles")

    val spark = SparkSession.builder
      .appName("ConsumerStreaming")
      .getOrCreate()

    val input = spark.read
      .option("header", "true")
      .option("sep", " ")
      .option("inferSchema", "true")
      .csv(pathToFiles + "order_date=" + args(0))

    val inputWithDate = addOrderDate(input)
    val aggregatedDf = processOrdersData(inputWithDate)
    aggregatedDf
      .write
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("dbtable", dbtable)
      .option("user", dbuser)
      .option("password", dbpassword)
      .mode(SaveMode.Append)
      .save()

    update("DELETE FROM " + dbtable + "_current_day" + " WHERE order_date = " + "'" + args(0)+ "'")
  }
}
