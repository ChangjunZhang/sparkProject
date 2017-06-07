package cn.itcast.spark.day7

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by changjun.zhang on 2017/6/2.
  */
object FilterUtils {

  /**
    * 注意不能使用SimpleDateFormat,因为他是线程不安全的,FastDateFormat是线程安全的
    */
  val dateFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")

  /**
    * 根据fields的时间字段过滤符合指定时间段的数据
    * @param fields
    * @param startTime
    * @param endTime
    * @return
    */
  def filterByTime(fields:Array[String],startTime:Long,endTime:Long):Boolean={
    val time = fields(1)
    val logTime = dateFormat.parse(time).getTime
    logTime >=startTime&&logTime<endTime
  }

  /**
    * 根据事件类型过滤
    * @param fields
    * @param eventType
    * @return
    */
  def filterByType(fields:Array[String],eventType:String): Boolean ={
    val _type = fields(0)
    eventType == _type
  }

  /**
    * 根据多个事件类型过滤
    * @param fields
    * @param eventType
    * @return
    */
  def filterByTypes(fields:Array[String],eventType:String*): Boolean ={
    val _type = fields(0)
    for(et<- eventType){
      if(_type==et)
        return true
    }
    false
  }

  def filterByTypeAndTime(fields:Array[String],eventType: String,beginTime:Long,endTime:Long):Boolean={
    val _type = fields(0)
    val _time = fields(1)
    val logTime = dateFormat.parse(_time).getTime
    eventType==_type&&logTime >=beginTime&&logTime<endTime
  }
}
