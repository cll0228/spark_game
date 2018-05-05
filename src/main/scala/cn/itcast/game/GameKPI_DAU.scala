package cn.itcast.game

import cn.itcast.game.log.LoggerLevels
import cn.itcast.game.util.{FilterUtils, TimeUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GameKPI_DAU {
  LoggerLevels.setStreamingLogLevels()
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DNU").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //读取数据，读取hdfs数据，因为我们离线处理的数据要求完整，不需要做预处理，所以读取的是离线hdfs的数据，最重要的原因是我们最终可以将程勋运行在yarn上
    val fileFields: RDD[Array[String]] = sc.textFile("hdfs://min1:9000/gamelogs/gamelogs.txt").map(_.split("\\|"))
    //创建开始和结束时间
    val startTime = TimeUtils("2016-02-01 00:00:00")
    val endTime = TimeUtils.getCertainDayTime(+1)

    val filterByTime = fileFields.filter(fields => FilterUtils.filterByTime(fields,startTime,endTime))

    //根据游戏的类进行过滤，（注册、登陆）,要将一天登陆登陆登出几次的人进行去重
    val filterByType: RDD[Array[String]] = filterByTime.filter(fileds=>FilterUtils.filterByTypes(fileds,EventType.LOGIN,EventType.REGISTER))
    val value: RDD[String] = filterByType.map(_(3)).distinct()
    value.foreach(println)
    println(value.count())
  }
}
