import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object UserIDMap {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()

    val df = spark.read.parquet("C:\\Users\\clark\\Desktop\\DataSource\\usertest.parquet")
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://120.52.176.207:3306/ald_xinen?user=test&password=aldwx123&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false"
    val username = "test"
    val password = "aldwx123"
    val dbtable = "uuid_map"
    val uuid_map = spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", username)
      .option("password", password)
      .option("dbtable", dbtable)
      .load().select("uuid")
    uuid_map.createTempView("uuid_map")
    df.select("uu","ak").distinct().createTempView("df")
    val df2 = spark.sql(
      """
        |select uu,ak from
        |(select  a.uu,a.ak,b.uuid from df a left join uuid_map b on a.uu=b.uuid) tmp
        |where uuid is null
      """.stripMargin)
    df2.show()

    df2.foreachPartition(itr => {
      val conn = DriverManager.getConnection(url, username, password)
      val ps2 = conn.prepareStatement("insert into uuid_map(uuid,ak) values(?,?)")
      itr.foreach(row => {
        val uuid = row.getString(0)
        val ak = row.getString(1)
        if (uuid != null) {
          ps2.setString(1, uuid)
          ps2.setString(2, ak)
          ps2.addBatch()
        }
      })
      ps2.executeBatch()
      conn.close()

    })
    spark.stop()
  }
}
