
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object UserTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://abc:3306/abc?user=abc&password=abc&useUnicode=true&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false"
    val username = "abc"
    val password = "abc"
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
    IDs.collect().foreach(id => {
      bitMap.setBit(id)
    })
    bitMap.show
val resultRDD =spark.sparkContext.makeRDD(bitMap.array)
    resultRDD.saveAsTextFile("C:\\Users\\clark\\Desktop\\DataSource\\BitMap")
  }
}
