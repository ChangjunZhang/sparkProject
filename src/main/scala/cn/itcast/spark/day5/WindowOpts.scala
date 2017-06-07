package cn.itcast.spark.day5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

/**
  * Created by changjun.zhang on 2017/5/24.
  */
object WindowOpts {
  //设置日志级别为WARN,方便查看打印的信息


  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WindowOpts").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Milliseconds(5000))
    val lines = ssc.socketTextStream("node1",8888)
    val pairs = lines.flatMap(_.split(" ")).map((_,1))
    val windowedWordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(15),Seconds(10))
    val a = windowedWordCounts.map(_._2).reduce(_+_)
    a.foreachRDD(rdd=>{
      println(rdd.take(0))
    })
    a.print()
    windowedWordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
