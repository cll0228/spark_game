package cn.itcast.game

import java.text.DecimalFormat

import cn.itcast.game.log.LoggerLevels
import cn.itcast.game.util.{FilterUtils, TimeUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GameKPI_UOS {
  LoggerLevels.setStreamingLogLevels()
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UOS").setMaster("local")
    val sc = new SparkContext(conf)

    val fileFields = sc.textFile("hdfs://min1:9000/gamelogs/gamelogs.txt").map(_.split("\\|"))
    //创建开始和结束时间
    val startTime = TimeUtils("2016-02-01 00:00:00")
    val endTime0202 = TimeUtils.getCertainDayTime(+1)
    val endTime0203 = TimeUtils.getCertainDayTime(+2)

    //先对数据进行时间过滤，加入统计的2月1号的新增用户
    val filterByTime0201: RDD[Array[String]] = fileFields.filter(fields=>FilterUtils.filterByTime(fields,startTime,endTime0202))
    //根据游戏的类型进行过滤，要将一天登陆登出几次的人进行去重
    //0201注册和登陆的用户
    val dau0201: RDD[(String, Int)] = filterByTime0201.filter(fields=>FilterUtils.filterByTypes(fields,EventType.REGISTER,EventType.LOGIN)).map(_(3)).distinct().map((_,1))
    //统计0201次日留存，需要统计出0202登陆的玩家，拿到0202登陆的玩家和0201登陆注册的玩家进行join操作，join上的用户就表示这两天他都在玩游戏，这就是0202的次日留存
    //计算0202登陆的用户
    val fileterByTime0202: RDD[Array[String]] = fileFields.filter(fields=>FilterUtils.filterByTime(fields,endTime0202,endTime0203))
    val dau0202: RDD[(String, Int)] = fileterByTime0202.filter(fields=>FilterUtils.filterByType(fields,EventType.LOGIN)).map(_(3)).distinct().map((_,1))
    //统计0202的登陆用户人数
    val count0202: Long = dau0202.count()
    //求0201的次日留存人数，也就是0202的留存人数
    val joinResult: Double = dau0201.join(dau0202).count()

    //0202留存率
   val d: Double = joinResult/count0202

    val df = new DecimalFormat("0.00")
    print(df.format(d))
  }
}
