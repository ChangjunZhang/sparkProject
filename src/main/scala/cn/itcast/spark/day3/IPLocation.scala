package cn.itcast.spark.day3

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by changjun.zhang on 2017/5/4.
  */
object IPLocation {

  val data2MySQL = (iterator:Iterator[(String,Int)])=>{
    var conn:Connection = null
    var ps:PreparedStatement = null
    val sql = "INSERT INTO location_info(location,counts,access_date) VALUES (?, ?, ?)"
      try {
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "123456")
        iterator.foreach(line => {
          ps = conn.prepareStatement(sql)
          ps.setString(1, line._1)
          ps.setInt(2, line._2)
          ps.setDate(3, new Date(System.currentTimeMillis()))
          ps.executeUpdate()
        })
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (ps != null)
          ps.close()
      if (conn != null)
        conn.close()
    }
  }

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 2分法查找,返回索引
    * @param lines
    * @param ip
    * @return
    */
  def binarySearch(lines: Array[(String, String, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("IPLocation")
    val sc = new SparkContext(conf)
    val ipRulesRdd = sc.textFile("D://study//ip.txt").map(line=>{
      val fields  = line.split("\\|")
      val startNum =  fields(2)
      val endNum =  fields(3)
      val province =  fields(6)
      (startNum,endNum,province)
    })
    //全部IP映射规则collect到driver端
    val ipRulesArr = ipRulesRdd.collect()

    //广播规则:将规则广播到执行该程序的所有work节点
    val ipRulesBroadcast = sc.broadcast(ipRulesArr)
    val ipsRdd = sc.textFile("d://study//access_log").map(line=>{

      val fields = line.split("\\|")
      fields(1)
    })

    val result = ipsRdd.map(ip=>{
      val ipNum = ip2Long(ip)
      val index = binarySearch(ipRulesBroadcast.value,ipNum)
      val info = ipRulesBroadcast.value(index)
      //(ip的起始num,ip的结束num,省份名)
      info
    }).map(t=>(t._3,1)).reduceByKey(_+_)
    result.foreachPartition(data2MySQL)
    println(result.collect().toBuffer)
    sc.stop()
  }
}

