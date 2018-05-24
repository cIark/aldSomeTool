
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object UserTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://120.52.176.207:3306/ald_xinen?user=test&password=aldwx123&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false"
    val username = "test"
    val password = "aldwx123"
    val dbtable = "uuid_map"


    val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getName).getOrCreate()

    val uuDF = spark.read.parquet("C:\\Users\\clark\\Desktop\\DataSource\\usertest.parquet").select("uu").distinct()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", username)
      .option("password", password)
      .option("dbtable", dbtable)
      .load()
import spark.implicits._
    val bitMap = new BitMap(5)

    val IDdf = uuDF.join(jdbcDF, uuDF("uu") === jdbcDF("uuid")).select("id")
    val IDs = IDdf.map(row => row.get(0).toString.toInt)
    IDs.collect().foreach(id=>{
      bitMap.setBit(id)})
    bitMap.show
    println(bitMap.count)
  }
}
