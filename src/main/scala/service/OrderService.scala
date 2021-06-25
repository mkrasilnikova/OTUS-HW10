package service

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object OrderService {

  def addOrderDate(df: DataFrame): DataFrame = {
    df.withColumn("order_date", to_date(col("order_date_time"), "dd/MM/yyyy HH:mm"))
  }

  def addOrderDateFormatted(df: DataFrame): DataFrame = {
    df.withColumn("order_date", date_format(col("order_date"), "yyyy-MM-dd"))
  }

  def processOrdersData(orders: DataFrame): DataFrame = {
    orders
      .withColumn("product_total_price", col("quantity") * col("product_price"))
      .groupBy(col("order_date"), col("order_number"))
      .agg(
        round(sum("product_total_price"), 2).as("total_order_price"),
      )
  }
}
