package cn.itcast.spark.day5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by changjun.zhang on 2016/5/21.
  */
object StateFulWordCount {

  //设置日志级别为WARN,方便查看打印的信息
  Logger.getLogger("org").setLevel(Level.WARN)

  /**
    * Seq这个批次某个单词的次数
    * Option[Int]：以前的结果
    * 注意:***参与过运算的key即使本批次没有值也会参与循环,例如:第一批(hello,CompactBuffer(1, 1),None),第二批:it==========(hello,CompactBuffer(1),Some(2))
    */
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //方式1
    //iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))

    //方式2模式匹配
    iter.map{ case(word, current_count, history_count) => (word, current_count.sum + history_count.getOrElse(0)) }
  }

  def main(args: Array[String]) {
    //StreamingContext
    val conf = new SparkConf().setAppName("StateFulWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    /**
      *  updateStateByKey必须设置checkpointDir,否则会跑出异常
      *  多个节点计算时需要指定一个共享存储,如hdfs,这里只有一个节点,因此使用本地路径
      */
    sc.setCheckpointDir("D://ck")
    val ssc = new StreamingContext(sc, Seconds(5))

    val ds = ssc.socketTextStream("node1", 8888)
    //DStream是一个特殊的RDD
    //hello tom hello jerry
    val result = ds.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(sc.defaultParallelism), true)
    result.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
