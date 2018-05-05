package cn.itcast.game

import cn.itcast.game.log.LoggerLevels
import cn.itcast.game.util.{FilterUtils, TimeUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GameKPI_DNU {
  LoggerLevels.setStreamingLogLevels()
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DNU").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //读取数据，读取hdfs数据，因为我们离线处理的数据要求完整，不需要做预处理，所以读取的是离线hdfs的数据，最重要的原因是我们最终可以将程勋运行在yarn上
    val fileFields: RDD[Array[String]] = sc.textFile("hdfs://min1:9000/gamelogs/gamelogs.txt").map(_.split("\\|"))
    //创建开始和结束时间
    val startTime = TimeUtils("2016-02-01 00:00:00")
    val endTime = TimeUtils.getCertainDayTime(+1)
    //先对数据进行时间过滤，加入我们统计2016-2-1日的新增用户
    val filterByTime: RDD[Array[String]] = fileFields.filter(fields => FilterUtils.filterByTime(fields,startTime,endTime))
    //再对一天的数据进行过滤，取出新增用户，eventtype = 1
    val filterByType: RDD[Array[String]] = filterByTime.filter(fields => FilterUtils.filterByType(fields,EventType.REGISTER))

    //剩下的就是当日的新增用户
    println(filterByType.count())
    sc.stop()
  }
}
