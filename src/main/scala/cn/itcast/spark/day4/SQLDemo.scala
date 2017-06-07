package cn.itcast.spark.day4

//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.{SQLContext, SparkSession}

/**
  * Created by changjun.zhang on 2017/5/15.
  */
object SQLDemo {
  def main(args: Array[String]): Unit = {

    /*val conf = new SparkConf().setAppName("SQLDemo").setMaster("local[2]")
    val sc  = new SparkContext(conf)
    val spark = SparkSession.builder().appName("SQLDemo").getOrCreate()
    System.setProperty("user.name","root")
    val personRDD = sc.textFile("hdfs://node1:9000/person.txt").map(line=>{
      val fields = line.split(",")
      Person(fields(0).toLong,fields(1),fields(2).toInt)
    })
    import spark.implicits._
    val personDF = personRDD.toDF
    personDF.createOrReplaceTempView("t_person")
    spark.sql("select * from t_person where age>=20 order by age desc limit 2").show()
    sc.stop()*/
  }

}
case class Person(id:Long,name:String,age:Int)