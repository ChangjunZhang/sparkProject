package cn.itcast.spark.day5

import java.net.InetSocketAddress

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumePollWordCount {
  //设置日志级别为WARN,方便查看打印的信息
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FlumePollWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //从flume中拉取数据(flume的地址)
    val address = Seq(new InetSocketAddress("node1", 8888))
    val flumeStream = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_AND_DISK)
    val words = flumeStream.flatMap(x => new String(x.event.getBody().array()).split("\t")).map((_,1))
    val results = words.reduceByKey(_+_)
    results.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
