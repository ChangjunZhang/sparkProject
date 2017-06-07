package cn.itcast.spark.day5

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by changjun.zhang on 2017/5/19.
  */
object StateFullWordCount {
  //设置日志级别为WARN,方便查看打印的信息
  Logger.getLogger("org").setLevel(Level.WARN)

  /**
    *  inter中String是单词,Seq是本批次的单词计数集合,Option是以前的累加结果
    */
  //分好组的数据
  val updateFunc1 = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    //iter.map{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
    iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
    //iter.map{ case(word, current_count, history_count) => (word, current_count.sum + history_count.getOrElse(0)) }
  }
  val updateFunc= (inter:Iterator[(String, Seq[Int], Option[Int])]) => {
    //方式一
    //inter.flatMap(it=>Some(it._2.sum+it._3.getOrElse(0)).map(x=>(it._1,x)))
    /**
      * 为了方便查看各个算子后数据的格式,将操作分开打印
      * 注意:***参与过运算的key即使本批次没有值也会参与循环,例如:第一批(hello,CompactBuffer(1, 1),None),第二批:it==========(hello,CompactBuffer(1),Some(2))
      */
    val res =inter.flatMap(
      //it===(hello,CompactBuffer(1, 1),None)
      it=>{
      //res1=========Some(2)
      val res1 = Some(it._2.sum+it._3.getOrElse(0))
      //res2=========Some((hello,2))
      val res2 =res1.map(x=>(it._1,x))

      res2
    })
    println("res===="+res)
    res


   /* //方式二:采用模式匹配
    inter.flatMap{
      case(x,y,z)=>Some(y.sum+z.getOrElse(0)).map(m=>(x,m))
    }*/

  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    //updateStateByKey必须设置checkpointDir,否则会跑出异常
    //多个节点计算时需要指定一个共享存储,如hdfs,这里只有一个节点,因此使用本地路径
    sc.setCheckpointDir("D://study/ck")

    //从指定的socket端口接收数据
    val ds = ssc.socketTextStream("node1",8888)
    //DStream是一个特殊的RDD
    val result = ds.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc1,new HashPartitioner(sc.defaultMinPartitions),true)
    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
