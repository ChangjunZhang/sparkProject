package cn.itcast.spark.day5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by changjun.zhang on 2017/5/24.
  */
object KafkaWordCount {

  //设置日志级别为WARN,方便查看打印的信息


  Logger.getLogger("org").setLevel(Level.ERROR)

  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i)) }
  }


  def main(args: Array[String]) {

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("D://ck")
    //"alog-2016-04-16,alog-2016-04-17,alog-2016-04-18"
    //"Array((alog-2016-04-16, 2), (alog-2016-04-17, 2), (alog-2016-04-18, 2))"
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    val words = data.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
