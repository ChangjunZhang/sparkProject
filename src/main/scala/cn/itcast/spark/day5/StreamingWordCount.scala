package cn.itcast.spark.day5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by changjun.zhang on 2017/5/18.
  */
object StreamingWordCount {
  //设置日志级别为WARN,方便查看打印的信息
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //从指定的socket端口接收数据
    val ds = ssc.socketTextStream("node1",8888)
    //DStream是一个特殊的RDD
    val result = ds.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    //打印结果
    result.print()

    //启动SSC
    ssc.start()
    ssc.awaitTermination()
  }
}
