import java.util.regex.Pattern
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object ETLJsonLocal {
  val fieldList =
    Array("client_ip", "(([01]?\\d?\\d|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d?\\d|2[0-4]\\d|25[0-5])") ::
      Array("ak", "\\w{32}") ::
      Array("at", "\\d+") ::
      Array("city", "[\\u4e00-\\u9fa5a-zA-Z0-9]+|(\\s&&[^\\f\\n\\r\\t\\v])*") ::
      Array("ev", "\\w+") ::
      Array("lang", "\\w+") ::
      Array("nt", "\\w+") ::
      Array("province", "[\\u4e00-\\u9fa5a-zA-Z0-9]+|(\\s&&[^\\f\\n\\r\\t\\v])*") ::
      Array("scene", "[\\s\\S+]*") ::
      Array("uu", "\\d+") ::
      Array("v", "[BDM]?\\d+(\\.\\d+){0,2}") ::
      Array("wv", "\\d+\\.\\d+\\.\\d+") ::
      Array("pm", "[\\s\\S+]*") ::
      Nil
  val field_list2 = List("sv", "dr", "wsdk", "qr", "wsr_query_aldsrc", "wsr_query_ald_share_src", "ifp", "ifo", "tp", "pp", "lp", "path")
  val schema =
    StructType(
      StructField("client_ip", StringType, true) ::
        StructField("ak", StringType, true) ::
        StructField("at", StringType, true) ::
        StructField("city", StringType, true) ::
        StructField("ev", StringType, true) ::
        StructField("lang", StringType, true) ::
        StructField("nt", StringType, true) ::
        StructField("province", StringType, true) ::
        StructField("scene", StringType, true) ::
        StructField("uu", StringType, true) ::
        StructField("v", StringType, true) ::
        StructField("wv", StringType, true) ::
        StructField("pm", StringType, true) ::
        StructField("sv", StringType, true) ::
        StructField("dr", StringType, true) ::
        StructField("wsdk", StringType, true) ::
        StructField("qr", StringType, true) ::
        StructField("wsr_query_aldsrc", StringType, true) ::
        StructField("wsr_query_ald_share_src", StringType, true) ::
        StructField("ifp", StringType, true) ::
        StructField("ifo", StringType, true) ::
        StructField("tp", StringType, true) ::
        StructField("pp", StringType, true) ::
        StructField("lp", StringType, true) ::
        StructField("path", StringType, true) ::
        StructField("st", StringType, true) ::
        StructField("day", StringType, true) ::
        StructField("hour", StringType, true) ::
        StructField("ag", StringType, true) ::
        StructField("ald_link_key", StringType, true) ::
        StructField("ald_position_id", StringType, true) ::
        StructField("ald_media_id", StringType, true) ::
        StructField("ct", StringType, true) ::
        StructField("error_message", StringType, true) ::
        StructField("ct_path", StringType, true) ::
        StructField("ct_chain", StringType, true) ::
        Nil)


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getName)
      .getOrCreate()

    val file = spark.read.text("C:\\Users\\clark\\Desktop\\DataSource\\json20.json")
    val today = "2018-04-01"
    val hour ="00"
      val result = execute(file, today, hour)
      val df = spark.createDataFrame(result, schema)
//    df.show()
      df.write.parquet("C:\\Users\\clark\\Desktop\\DataSource\\parquet1")
//    })
    spark.stop()
  }


  /**
    * 执行过滤的具体操作
    */
  def execute(file: DataFrame, today: String, hour: String): RDD[Row] = {
    val result: RDD[Row] = file.toJSON.rdd.map(line => {
      val result_list = scala.collection.mutable.ListBuffer[String]()
      var row = Row("")
      try {
        val json_line = JSON.parseObject(line).get("value").toString
        val json_object = JSON.parseObject(json_line)
        if (is_usefulJson(json_object)) {
          fieldList.foreach(field => {
            result_list.+=(json_object.get(field(0)).toString.trim)
          })
          field_list2.map(field => {
            if (json_object.containsKey(field)) {
              val field_value = json_object.get(field).toString.trim
              field_value.replaceAll("\\\\", "")
              result_list.+=(field_value)
            } else {
              result_list.+=(field)
            }
          })

          putStDayHour(json_object, result_list, today, hour)
          handleAG(json_object, result_list)
          handleCT(json_object, result_list)
          //
        } else {
          print("不是有用的json")
          println(json_line)
        }
        if (result_list.length == 36) {
          row = Row(result_list(0), result_list(1), result_list(2), result_list(3), result_list(4), result_list(5)
            , result_list(6), result_list(7), result_list(8), result_list(9), result_list(10), result_list(11)
            , result_list(12), result_list(13), result_list(14), result_list(15), result_list(16), result_list(17)
            , result_list(18), result_list(19), result_list(20), result_list(21), result_list(22), result_list(23)
            , result_list(24), result_list(25), result_list(26), result_list(27), result_list(28), result_list(29)
            , result_list(30), result_list(31), result_list(32), result_list(33), result_list(34), result_list(35))
        }
      }
      catch {
        case _: Exception => println("不是正确的json文件")
      }
      row
    }).filter(_.toString.length > 2)
    result
  }

  def is_matched(regex: String, field: String): Boolean = {
    val compile = Pattern.compile(regex)
    //匹配数据
    val matcher = compile.matcher(field)
    matcher.matches
  }

  def is_usefulJson(json_object: JSONObject): Boolean = {
    fieldList.map(field => {
      json_object.containsKey(field(0))
    }).reduce(_ && _) && fieldList.map(field => {
      is_matched(field(1), json_object.get(field(0)).toString.trim)
    }).reduce(_ && _)
  }

  def putStDayHour(json_object: JSONObject, result_list: ListBuffer[String], today: String, hour: String): ListBuffer[String] = {
    if (json_object.containsKey("st") && is_matched("\\d{13}", json_object.get("st").toString.trim)) {
      val st = json_object.get("st").toString.trim
      if (TimeUtil.st2Day(st.toLong).replaceAll("-", "") == today) {
        if (TimeUtil.st2hour(st.toLong) == hour) {
          result_list.+=(st)
          result_list.+=(TimeUtil.st2Day(st.toLong)) //day
          result_list.+=(TimeUtil.st2hour(st.toLong)) //hour
        } else {
          val time = today + hour + "0000"
          val timestap = TimeUtil.time2st(time)
          result_list.+=(timestap)
          result_list.+=(today)
          result_list.+=(hour)
        }
      } else {
        result_list.+=("st") //st
        result_list.+=("day") //day
        result_list.+=("hour") //hour
      }
    } else if (json_object.containsKey("ts") && is_matched("\\d{13}", json_object.get("ts").toString.trim)) {
      val st = json_object.get("ts").toString.trim
      if (TimeUtil.st2Day(st.toLong).replaceAll("-", "") == today) {
        if (TimeUtil.st2hour(st.toLong) == hour) {
          result_list.+=(st)
          result_list.+=(TimeUtil.st2Day(st.toLong)) //day
          result_list.+=(TimeUtil.st2hour(st.toLong)) //hour
        } else {
          val time = today + hour + "0000"
          val timestap = TimeUtil.time2st(time)
          result_list.+=(timestap)
          result_list.+=(today)
          result_list.+=(hour)
        }
      } else {
        result_list.+=("st") //st
        result_list.+=("day") //day
        result_list.+=("hour") //hour
      }
    } else {
      result_list.+=("st") //st
      result_list.+=("day") //day
      result_list.+=("hour") //hour
    }
    result_list
  }

  def handleAG(json_object: JSONObject, result_list: ListBuffer[String]): Unit = {
    if (json_object.containsKey("ag") && json_object.get("ag") != null) {
      val ag = json_object.get("ag").toString.trim
      ag.replaceAll("\"", "\'")
      ag.replaceAll("\\\\", "")
      if (ag.contains("{")) {
        val ag_json = JSON.parseObject(ag)
        if (ag_json.containsKey("ald_link_key") && ag_json.containsKey("ald_position_id") && ag_json.containsKey("ald_media_id")) {
          result_list.+=(ag)
          result_list.+=(ag_json.get("ald_link_key").toString)
          result_list.+=(ag_json.get("ald_position_id").toString)
          result_list.+=(ag_json.get("ald_media_id").toString)
        } else {
          result_list.+=(ag)
          result_list.+=("ald_link_key")
          result_list.+=("ald_position_id")
          result_list.+=("ald_media_id")
        }
      } else {
        result_list.+=(ag)
        result_list.+=("ald_link_key")
        result_list.+=("ald_position_id")
        result_list.+=("ald_media_id")
      }
    } else {
      result_list.+=("ag")
      result_list.+=("ald_link_key")
      result_list.+=("ald_position_id")
      result_list.+=("ald_media_id")
    }


  }

  def handleCT(json_object: JSONObject, result_list: ListBuffer[String]): Unit = {
    if (json_object.containsKey("ct") && json_object.get("ct") != null) {
      val ct = json_object.get("ct").toString.trim
      ct.replaceAll("\"", "\'")
      ct.replaceAll("\\\\", "")
      if (ct.toUpperCase.indexOf("ERROR") != -1) {
        result_list.+=(ct)
        result_list.+=(ct) //error字段设为和ct字段相同的值
        result_list.+=("ct_path")
        result_list.+=("ct_chain")
      } else if (ct.contains("{")) {
        val ag_json = JSON.parseObject(ct)
        if (ag_json.containsKey("path") && ag_json.containsKey("chain")) {
          result_list.+=(ct)
          result_list.+=("error_message")
          result_list.+=(ag_json.get("path").toString.trim)
          result_list.+=(ag_json.get("chain").toString.trim)
        } else {
          result_list.+=(ct)
          result_list.+=("error_message")
          result_list.+=("ct_path")
          result_list.+=("ct_chain")
        }
      } else {
        result_list.+=(ct)
        result_list.+=("error_message")
        result_list.+=("ct_path")
        result_list.+=("ct_chain")
      }


    } else {
      result_list.+=("ct")
      result_list.+=("error_message")
      result_list.+=("ct_path")
      result_list.+=("ct_chain")

    }


  }
}
