package cn.itcast.spark.day4

/*import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}*/

/**
  * Created by changjun.zhang on 2017/5/16.
  */
object SpecifyingSchema {
  def main(args: Array[String]): Unit = {
  /*  System.setProperty("user.name","root")
    val conf = new SparkConf().setAppName("SparkSQL2").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext =SparkSession.builder().appName("SparkSQL2").getOrCreate()
    //读取数据创建RDD
    val personRDD = sc.textFile("hdfs://node1:9000/person.txt").map(_.split(","))
    //通过StructType直接指定每个字段的schema
    val schema = StructType(List(
      StructField("id",IntegerType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true)
    ))
    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p=>Row(p(0).toInt,p(1).trim,p(2).toInt))
    //将schema的信息应用到roeRDD上
    val personDataFrame  = sqlContext.createDataFrame(rowRDD,schema)
    //注册为表
    personDataFrame.createOrReplaceTempView("t_person")
    //执行sql
    val df = sqlContext.sql("select * from t_person order by age desc ")
    df.write.json("hdfs://node1:9000/res1")
    //停止sparkContext
    sc.stop()*/

  }

}
