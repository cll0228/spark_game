package cn.itcast.game.util

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by root on 2016/7/10.
  *
  * 我们定义的单例对象，每个Executor进程中只有一个FilterUtils实例
  * 但是Executor进程中的Task是多线程的
  */
object FilterUtils {

  //为什么不用SimpleDateFormat,因为SimpleDateFormat不是线程安全的
  val dateFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")


  def filterByTime(fields: Array[String], startTime: Long, endTime: Long): Boolean = {
    if(fields.length>1){
      val time = fields(1)
      val logTime = dateFormat.parse(time).getTime
      logTime >= startTime && logTime < endTime
    }else{
      false
    }
  }

  def filterByType(fields: Array[String], eventType: String) = {
    val _type = fields(0)
    _type == eventType
  }

  def filterByTypeAndTime(fields: Array[String], eventType: String, beginTime: Long, endTime: Long) = {
    val _type = fields(0)
    val time = fields(1)
    val timeLong = dateFormat.parse(time).getTime
    _type == eventType && timeLong >= beginTime && timeLong < endTime
  }

  def filterByTypes(fields: Array[String], eventTypes: String*): Boolean = {
    val _type = fields(0)
    for(et <- eventTypes){
      if(_type == et)
        return true
    }
    false
  }

}
