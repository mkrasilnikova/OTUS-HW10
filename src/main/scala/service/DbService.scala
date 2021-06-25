package service

import com.typesafe.config.ConfigFactory

import java.sql.{DriverManager, Statement}

object DbService {
  val config = ConfigFactory.load
  val driver = config.getString("driver")
  val url = config.getString("url")
  val dbtable = config.getString("dbtable")
  val dbuser = config.getString("dbuser")
  val dbpassword = config.getString("dbpassword")


  def update(query: String): Unit =
    execute(_.executeUpdate(query))

  private def execute[T](fn: Statement => T): T = {
    val conn = DriverManager.getConnection(url, dbuser, dbpassword)
    try {
      val stm = conn.createStatement()
      fn(stm)
    } finally {
      conn.close()
    }
  }
}
