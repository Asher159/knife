package org.foxconn.knife

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable

object nowWinRate {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("nowWinRate").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    var map = mutable.HashMap[String, Int]()
    var set = mutable.HashSet[String]()
    import spark.implicits._
    val url = "jdbc:mysql://localhost:3306/nba?useSSL=false&serverTimezone=Asia/Shanghai"
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    val jdbcDF = spark.read.jdbc(url, "(SELECT `球队`,`球员`,`出场`,`首发`,`时间`,`得分`,`犯规`,`失误`,`盖帽`,`抢断`,`助攻`,`篮板` " +
      "FROM player WHERE `出场` > 4 AND `首发` >4 AND `时间`>10) T", properties)


// 依次取出满足条件的球员加入set, 如果set中没有该球员，则map加1
    val score50 = jdbcDF.orderBy(desc("得分")).take(50)
    score50.foreach(Row => {
      val name = Row.get(0).toString
      val player = Row.get(1).toString
      if (set.add(player))
        map(name) = map.getOrElse(name, 0) + 1
    })

    val illegality5 = jdbcDF.orderBy("犯规").take(5)
    illegality5.foreach(Row => {
      val name = Row.get(0).toString
      val player = Row.get(1).toString
      if (set.add(player))
        map(name) = map.getOrElse(name, 0) + 1
    })

    val mistake5 = jdbcDF.orderBy("失误").take(5)
    mistake5.foreach(Row => {
      val name = Row.get(0).toString
      val player = Row.get(1).toString
      if (set.add(player))
        map(name) = map.getOrElse(name, 0) + 1
    })

    val cap10 = jdbcDF.orderBy(desc("盖帽")).take(10)
    cap10.foreach(Row => {
      val name = Row.get(0).toString
      val player = Row.get(1).toString
      if (set.add(player))
        map(name) = map.getOrElse(name, 0) + 1
    })

    val steal11 = jdbcDF.orderBy(desc("抢断")).take(11)
    steal11.foreach(Row => {
      val name = Row.get(0).toString
      val player = Row.get(1).toString
      if (set.add(player))
        map(name) = map.getOrElse(name, 0) + 1
    })

    val help10 = jdbcDF.orderBy(desc("助攻")).take(10)
    help10.foreach(Row => {
      val name = Row.get(0).toString
      val player = Row.get(1).toString
      if (set.add(player))
        map(name) = map.getOrElse(name, 0) + 1
    })

    val backboard10 = jdbcDF.orderBy(desc("篮板")).take(10)
    backboard10.foreach(Row => {
      val name = Row.get(0).toString
      val player = Row.get(1).toString
      if (set.add(player))
        map(name) = map.getOrElse(name, 0) + 1
    })

    print(map)
    println(map.size)
    //排 序 取前10
    map.toList.toDF().orderBy(desc("_2")).show(10)
  }

}
