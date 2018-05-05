package cn.itcast.game.iplocation

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017/9/18.
  */
object IPLocation {

  def ip2Long(ip: String):Long = {
     val fragments =  ip.split("[.]")
    var ipNum = 0L
    //循环ip的4个数
    for(i<- 0 until fragments.length){
      ipNum  = fragments(i).toLong | ipNum<< 8L
    }
    ipNum
  }

  /**
    *二分查找法
    * 因为ip段中ip是有序的，所以采用二分法排序最快
    * @param lines
    * @param ip
    * @return
    */
  def binarySearch(lines: Array[(String, String, String)], ip: Long):Int = {

    var low = 0
    var high = lines.length - 1
    while (low <= high){
      val middle = (low+high)/2
      if((ip >= lines(middle)._1.toLong)&&(ip<=lines(middle)._2.toLong))
        return middle
      if(ip < lines(middle)._1.toLong){
        high = middle - 1
      }else{
        low = middle  +1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("iplocation")
    val sc = new SparkContext(conf)
    val ipNumRange2LocaltionRDD = sc.textFile("F:\\bdata\\spark\\day03\\扩展资料\\服务器访问日志根据ip地址查找区域\\ip.txt").map(_.split("\\|")).
      map(x => (x(2).toString, x(3).toString, x(6).toString))
    //    println(ipNumRange2LocaltionRDD.foreach(println))
    //将ip段的信息加载到内存
    val ipNumRange2LocaltionArray = ipNumRange2LocaltionRDD.collect()
    //广播ip端变量
    val broadcastArray = sc.broadcast(ipNumRange2LocaltionArray)
    //读取日志信息
    val ipLogRDD = sc.textFile("F:\\bdata\\spark\\day03\\扩展资料\\服务器访问日志根据ip地址查找区域\\20090121000132.txt").map(_.split("\\|"))
    //    println(ipLogRDD.foreach(println))
    //将日志信息的ip map出来
    val locationAndIp = ipLogRDD.map(_(1).toString).mapPartitions(
      it => { //匿名函数，it代表ip
        val arr = broadcastArray.value //获取ip段信息
        it.map(ip => {
          val ipNum = ip2Long(ip) //将ip转换为数字
          val index = binarySearch(arr, ipNum) //二分法查找ip地址在那个省份,返回在广播array中的角标
          val t = arr(index) //找到广播变量
          (t._3, ip) //返回广播变量和ip
        })
      })
    //统计省份的ip数
    val locationCount = locationAndIp.map(t => (t._1, 1)).reduceByKey(_ + _)
    locationCount.foreach(println)
//    locationCount.foreachPartition(data2MySQL)
    sc.stop()
  }

  def iplocation(ips: RDD[AnyRef],sc:SparkContext): Unit ={
    val ipNumRange2LocaltionRDD = sc.textFile("F:\\bdata\\spark\\day03\\扩展资料\\服务器访问日志根据ip地址查找区域\\ip.txt").map(_.split("\\|")).
      map(x => (x(2).toString, x(3).toString, x(6).toString))
    //    println(ipNumRange2LocaltionRDD.foreach(println))
    //将ip段的信息加载到内存
    val ipNumRange2LocaltionArray = ipNumRange2LocaltionRDD.collect()
    //广播ip端变量
    val broadcastArray = sc.broadcast(ipNumRange2LocaltionArray)

//    val locationAndIp = ips.map(_.toString).mapPartitions(
//      it => { //匿名函数，it代表ip
//        val arr = broadcastArray.value //获取ip段信息
//        it.map(ip => {
//          val ipNum = ip2Long(ip) //将ip转换为数字
//          val index = binarySearch(arr, ipNum) //二分法查找ip地址在那个省份,返回在广播array中的角标
//          val t = arr(index) //找到广播变量
//          (t._3, ip) //返回广播变量和ip
//        })
//      })

    val locationAndIp = ips.map(ip=>{
      val arr = broadcastArray.value //获取ip段信息
      val ipNum = ip2Long(ip.toString) //将ip转换为数字
                val index = binarySearch(arr, ipNum) //二分法查找ip地址在那个省份,返回在广播array中的角标
                if(index>0){
                  val t = arr(index) //找到广播变量
                  (t._3, ip) //返回广播变量和ip
                }else{
                  ("未知",ip)
                }
    })

    //统计省份的ip数
    val locationCount = locationAndIp.map(t => (t._1, 1)).reduceByKey(_ + _)
    locationCount.foreach(println)

  }

  def data2MySQL(iterator: Iterator[(String,Int)]):Unit = {
    var conn:Connection = null
    var ps:PreparedStatement = null
    val sql="INSERT INTO big_data.location_info(location,total_count) values(?,?)"
    try{
      conn = DriverManager.getConnection("jdbc:mysql://192.168.203.11:3306/big_data?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true","root","root")
      iterator.foreach(line=>{
        ps = conn.prepareStatement(sql)
      ps.setString(1,line._1)
        ps.setInt(2,line._2)
        ps.execute()
        println(line+"保存成功")
      })
    }catch {
      case e:Exception => println(e)
    }finally {
      if(ps != null)
        ps.close()
      if(conn != null)
        conn.close()
    }

  }
}
