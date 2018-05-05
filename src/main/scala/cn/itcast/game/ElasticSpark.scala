package cn.itcast.game
import cn.itcast.game.iplocation.IPLocation
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark可以从es中加载数据进行处理，也就是通过es进行一次预处理，数据集变小，再做计算
  *
  */
object ElasticSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("elascticspark").setMaster("local[2]")
      //设置读取es的地址信息
    conf.set("es.nodes","min1,min2,min3")
    conf.set("es.port","9200")
    conf.set("es.index.auto.create","true")
    val sc = new SparkContext(conf)
    val query =
      s"""{
       "query": {"match_all": {}},
       "_source":"ip"
     }"""
    //从es中读取数据，es可以帮助spark程序做一次预处理
    val rdd1: RDD[(String, collection.Map[String, AnyRef])] = sc.esRDD("gamelogs", query)
    //  取出ip的值
    val rdd2: RDD[collection.Map[String, AnyRef]] = rdd1.map(_._2)
    //qu出ip的值
    val ips: RDD[AnyRef] = rdd2.flatMap(x=>x.values)
    //过滤脏数据
    val resultips: RDD[AnyRef] = ips.filter(_.toString.split("\\.").length>3)
    IPLocation.iplocation(resultips,sc)
    sc.stop()
  }
}
