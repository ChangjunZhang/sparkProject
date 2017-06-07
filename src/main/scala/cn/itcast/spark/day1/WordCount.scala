package cn.itcast.spark.day1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by changjun.zhang on 2017/4/20.
  */
object WordCount {
  def main(args: Array[String]): Unit = {

    //非常重要,是同向spark集群的入口
    val conf =  new SparkConf().setAppName("WordCount").
      setJars(Array("D:\\WorkSpaces\\Study\\HelloSpark\\target\\hello-spark-1.0.jar")).
      setMaster("spark://node1:7077")

    val sc = new SparkContext(conf)
    //textFile会产生两个RDD:HadoopRDD->MapPartitionsRDD
    sc.textFile(args(0)).
      //产生一个RDD:MapPartitionsRDD
      flatMap(_.split(" ")).
      //产生一个RDD:MapPartitionsRDD
      map((_,1)).
      //产生一个RDD:ShuffledRDD
      reduceByKey(_+_)
      //产生一个RDD:MapPartitionsRDD
      .saveAsTextFile(args(1))
    sc.setCheckpointDir("hdfs://node1:9000/checkDir")
    val rdd = sc.textFile("hdfs://node1:9000/itcast")
    rdd.checkpoint()
    rdd.count()
    sc.stop()

  }
}
