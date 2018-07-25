package scala

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SaveWhiteList {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      //      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()
    val input = "hdfs://10.0.100.17:4007/white_list/20180724/*"
    val output = "hdfs://10.0.100.17:4007/tmp/whitelist_file1"
    val file = spark.read.text(input)
    val result: RDD[String] = file.toJSON.rdd.map(line => {
      try {
        val json_line = JSON.parseObject(line).get("value").toString
        val json_object = JSON.parseObject(json_line)
        if (json_object.getString("ak") == "dc3217b4ee52e505d495c08caa5d1843") {
          json_object.toJSONString
        } else {
          ""
        }
      } catch {
        case _: Exception => println("wrong json file!!")
          ""
      }
    }).filter(_.length > 2)
    result.saveAsTextFile(output)
    spark.stop()
  }
}
