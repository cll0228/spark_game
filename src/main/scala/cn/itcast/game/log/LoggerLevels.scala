package cn.itcast.game.log

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

/**
  * Created by Administrator on 2017/9/20.
  */
object LoggerLevels extends Logging {
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
