package cn.itcast.spark.day2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by changjun.zhang on 2017/5/2.
  */
object AdvUserLocation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AdvUserLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //ArrayBuffer((18688888888,16030401EAFB68F1E3CDF819735E1C66,-20160327082400), (18611132889,16030401EAFB68F1E3CDF819735E1C66,-20160327082500), (18688888888,16030401EAFB68F1E3CDF819735E1C66,20160327170000), (18611132889,16030401EAFB68F1E3CDF819735E1C66,20160327180000), (18611132889,9F36407EAD0629FC166F14DDE7970F68,-20160327075000), (18688888888,9F36407EAD0629FC166F14DDE7970F68,-20160327075100), (18611132889,9F36407EAD0629FC166F14DDE7970F68,20160327081000), (18688888888,9F36407EAD0629FC166F14DDE7970F68,20160327081300), (18688888888,9F36407EAD0629FC166F14DDE7970F68,-20160327175000), (18611132889,9F36407EAD0629FC166F14DDE7970F68,-20160327182000), (18688888888,9F36407EAD0629FC166F14DDE7970F68,20160327220000), (18611132889,9F36407EAD0629FC166F14DDE7970F68,20160327230000), (18611132889,CC0710CC94ECC657A8561DE549D940E0,-20160327081100), (18688888888,CC0710CC94ECC657A8561DE549D940E0,-20160327081200), (18688888888,CC0710CC94ECC657A8561DE549D940E0,20160327081900), (18611132889,CC0710CC94ECC657A8561DE549D940E0,20160327082000), (18688888888,CC0710CC94ECC657A8561DE549D940E0,-20160327171000), (18688888888,CC0710CC94ECC657A8561DE549D940E0,20160327171600), (18611132889,CC0710CC94ECC657A8561DE549D940E0,-20160327180500), (18611132889,CC0710CC94ECC657A8561DE549D940E0,20160327181500))
    val rdd0 = sc.textFile("D:\\study\\bs_log").map(line=>{
      val fields = line.split(",")
      val eventType =fields(3)
      val time  = fields(1)
      val timeLong = if(eventType=="1") -time.toLong else time.toLong
      ((fields(0),fields(2)),timeLong)
    })
    val rdd1 = rdd0.reduceByKey(_+_).map(t=>{
      val mobile = t._1._1
      val lac = t._1._2
      val time = t._2
      (lac,(mobile,time))
    })
    val rdd2 = sc.textFile("D:\\study\\lac_info.txt").map(line=>{
      val f = line.split(",")
      (f(0),(f(1),f(2)))
    })

    val rdd3 = rdd1.join(rdd2).map(t=>{
      val lac = t._1
      val mobile = t._2._1._1
      val time = t._2._1._2
      val x = t._2._2._1
      val y = t._2._2._2
      (mobile,lac,time,x,y)
    })
    //rdd4是分组后的
    val rdd4 = rdd3.groupBy(_._1)
    val rdd5 = rdd4.mapValues(it=>{
      it.toList.sortBy(_._3).reverse.take(2)
    })
    println(rdd5.collect().toBuffer)
    rdd5.saveAsTextFile("D://study//out")
    sc.stop()
  }
}
