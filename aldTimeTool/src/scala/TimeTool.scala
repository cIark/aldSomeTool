object TimeTool {
  import java.text.SimpleDateFormat
  import java.util.{Calendar, Date}

  object TimeTool {

    //获取月开始和结束时间
    val sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
    private val calendar = Calendar.getInstance()
    calendar.add(Calendar.MONTH, -1)
    calendar.set(Calendar.DAY_OF_MONTH, 1)
    private val lastMonthStart = sdf.format(calendar.getTime)
    calendar.add(Calendar.MONTH,1)
    calendar.set(Calendar.DAY_OF_MONTH,0)
    private val lastMonthEnd = sdf.format(calendar.getTime)

    def getLMS = day2Stamp(lastMonthStart.substring(0, 8))
    def getLME = day2Stamp(lastMonthEnd.substring(0, 8))

    /**
      * 时间字符串转时间戳总方法
      * @param stringTime
      * @param parse
      * @return
      */
    def strToStamp(stringTime: String, parse: String) = {
      val sdf = new SimpleDateFormat(parse)
      sdf.parse(stringTime).getTime
    }

    /**
      * 时间戳转时间字符串的总方法
      * @param stamp
      * @param parse
      * @return
      */
    def stampToString(stamp: Long, parse: String) = {
      val sdf = new SimpleDateFormat(parse)
      sdf.format(new Date(stamp))
    }
    //yyyyMMdd格式的转化
    def day2Stamp(stringTime:String) = strToStamp(stringTime, "yyyyMMdd")
    def stamp2Day(stamp:Long)=stampToString(stamp,"yyyyMMdd")


  }

}
