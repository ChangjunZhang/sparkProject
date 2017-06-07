package cn.itcast.spark.day3

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}


/**
  * Created by changjun.zhang on 2017/5/3.
  */
object UrlCountPartition {

  def main(args: Array[String]): Unit = {
    val conf= new SparkConf().setAppName("UrlCountPartition").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //rdd1将数据切分,元组中(URL,1)
    val rdd1 = sc.textFile("D:\\study\\itcast.log").map(line=>{
      val f = line.split("\t")
      (f(1),1)
    })
    val rdd2 = rdd1.reduceByKey(_+_)
    val rdd3 =rdd2.map(t=>{
      val url  = t._1
      val host = new URL(url).getHost
      (host,(url,t._2))
    })
    //rdd3.repartition(3).saveAsTextFile("D://study/out1")
    val ints = rdd3.map(_._1).distinct().collect()
    val hostPartitioner = new HostPartitioner(ints)

    //分区类
    val rdd4 = rdd3.partitionBy(hostPartitioner).mapPartitions(it=>{
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })
    rdd4.saveAsTextFile("D://study//out3")

    sc.stop()



  }
}

/**
  * 自定义分区
  * @param ints hostList
  * 根据host分区
  */
class HostPartitioner(ints:Array[String]) extends Partitioner{
  val partMap = new scala.collection.mutable.HashMap[String,Int]
  var count = 0;
  for(i<-ints){
    partMap += (i->count)
    count += 1
  }
  override def numPartitions: Int = ints.length

  override def getPartition(key: Any): Int = {
    partMap.getOrElse(key.toString,0)
  }
}