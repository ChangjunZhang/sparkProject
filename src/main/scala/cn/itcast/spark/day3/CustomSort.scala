package cn.itcast.spark.day3

import org.apache.spark.{SparkConf, SparkContext}

object OrderContext{
  implicit val girlOrdering = new Ordering[Girl]{
    override def compare(x: Girl, y: Girl): Int = {
      if(x.faceValue>y.faceValue)1
      else if(x.faceValue==y.faceValue){
        if(x.age>y.age) -1 else 1
      }else -1
    }
  }
}


/**
  * Created by changjun.zhang on 2017/5/3.
  * 自定义排序,先按照颜值排序,颜值相同按照年龄排
  * name,faceValue,age
  */
object CustomSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("yuihatano", 90, 28, 1), ("angelababy", 90, 27, 2),("JuJingYi", 95, 22, 3)))

    import OrderContext._
    val rdd2  = rdd1.sortBy(t=>{
      Girl(t._2,t._3)
    },false)
    print(rdd2.collect().toBuffer)
  }
}

/**
  * 第一种方式
  *  faceValue
  *  age

case class Girl(val faceValue:Int,val age:Int) extends Ordered[Girl] with Serializable{
  override def compare(that: Girl): Int = {
    if(this.faceValue==that.faceValue){
      that.age - this.age
    }else{
      this.faceValue - that.faceValue
    }
  }
}*/
case class Girl(faceValue:Int,age:Int) extends Serializable