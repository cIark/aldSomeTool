import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CountTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getName).getOrCreate()
    val resultRDD = spark.sparkContext.textFile("C:\\Users\\clark\\Desktop\\DataSource\\BitMap").collect()

    val array = resultRDD.map(x => x.toString.toInt)

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
      .load().select("id","uuid","ak")
    uuid_map.createTempView("uuid_tmp")
    def exist ={x:Int=> BitMap.exists(array,x)}
    spark.udf.register("exist",exist)
    val rs=spark.sql(
      """
        |select ak,uuid
        |from uuid_tmp
        |where exist(id)
      """.stripMargin)
    rs.groupBy("ak").count().show()


  }
}
