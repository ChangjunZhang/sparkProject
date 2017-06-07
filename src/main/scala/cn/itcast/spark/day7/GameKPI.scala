package cn.itcast.spark.day7

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by changjun.zhang on 2017/6/2.
  */
object GameKPI {
  //设置日志的级别方便调试
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val queryTime ="2016-02-02 00:00:00"
    val startTime = TimeUtils(queryTime)
    val endTime = TimeUtils.getCertainDayTime(1)
    val conf = new SparkConf().setAppName("GameKPI").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //切分之后的数据
    val splitedLogs = sc.textFile("D:\\study\\testData\\GameLog.txt").map(_.split("\\|"))

    //过滤后并缓存,以计算多个指标,当触发action的时候数据才会缓存
    val filteredLogs = splitedLogs.filter(fields=>FilterUtils.filterByTime(fields,startTime,endTime))
      .cache()

    //日新增用户数,Daily New Uers 缩写DNU
    val dnu = filteredLogs.filter(fields=>FilterUtils.filterByType(fields,EventType.REGISTER)).count()

    //日活跃用户(包括新增和登陆),Daily Active Uers 缩写DAU
    val dau = filteredLogs.filter(fields=>FilterUtils.filterByTypes(fields,EventType.REGISTER,EventType.LOGIN))
      .map(_(3))
      .distinct()
      .count()


    //留存率:某段事件的新增用户数记为A,经过一段时间后,仍然使用的用户占新增用户A的比例即为留存率
    //次日留存率:日新增用户再+1日登陆的用户占新增用户的比例
    //次日留存(Day 1 Retention Ratio)

    /**
      * 注意,这里的startTime是查询的日期,t1是往前推一天的日期
      */
    val t1 = TimeUtils.getCertainDayTime(-1)
    /**
      * 前一天的注册用户,map的作用是为了下一步统计留存率,做join操作,只key有意义,value并无意义,所以什么值都行
      */
    val lastDayRegUsers =splitedLogs.filter(fields=>FilterUtils.filterByTypeAndTime(fields,EventType.REGISTER,t1,startTime))
      .map(x=>(x(3),0))

    //从今天的数据中过滤是登陆行为的数据,并且去重复(因为一个用户一天可能登陆多次)
    val todayLoginUsers = filteredLogs.filter(fields=>FilterUtils.filterByType(fields,EventType.LOGIN))
      .map(x=>(x(3),0))
      .distinct()

    //第一天注册并且第二天登陆的用户数量
    val d1r :Double= lastDayRegUsers.join(todayLoginUsers).count()
    println(d1r)
    //用户一天的留存率
    val d1rr = d1r/lastDayRegUsers.count()

    println(d1rr)
    sc.stop()

  }
}


//create table GameKPI(id,gameName,zone,datetime,