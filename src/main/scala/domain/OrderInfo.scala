package domain

case class OrderInfo(order_number: Int,
                     order_date_time: String,
                     item_name: String,
                     quantity: Int,
                     product_price: Double,
                     total_products: Int
              )

object OrderInfo {
  def apply(a: Array[String]):OrderInfo = {
    OrderInfo (
      a(0).toInt,
      a(1),
      a(2),
      a(3).toInt,
      a(4).toDouble,
      a(5).toInt
    )
  }
}
