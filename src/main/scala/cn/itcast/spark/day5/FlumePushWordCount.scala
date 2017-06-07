package cn.itcast.spark.day5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by changjun.zhang on 2017/5/19.
  */
object FlumePushWordCount {

  //设置日志级别为WARN,方便查看打印的信息
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FlumePushWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    //推送方式:flume向spark发送数据
    val flumeStream  = FlumeUtils.createStream(ssc,"192.168.100.2",8888)
    //flume中的数据通过event.getBody才能拿到真正的内容
    val words = flumeStream.flatMap(x=>new String(x.event.getBody.array()).split("\t").map((_,1)))
    val results =words.reduceByKey(_+_)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
