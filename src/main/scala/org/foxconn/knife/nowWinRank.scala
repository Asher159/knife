package org.foxconn.knife

import java.sql.DriverManager
import java.time.{LocalDateTime, ZoneOffset}
import java.util
import org.apache.spark.SparkConf
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, regexp_extract, regexp_replace, split}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object nowWinRank {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("nowWinRank").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    // Mysql配置
    val driverClass = SparklUtil.getValueFromConfig("jdbc.driver.class")
    val url = "jdbc:mysql://localhost:3306/nba?useSSL=false&serverTimezone=Asia/Shanghai"
    val user = SparklUtil.getValueFromConfig("jdbc.user")
    val password = SparklUtil.getValueFromConfig("jdbc.password")
    val connection = () => {
      Class.forName(driverClass)
      DriverManager.getConnection(url, user, password)
    }
    // sql语句
    val selectSQL = "SELECT `球队`,`时间`,`结果`,`主客`,`比赛` FROM nba_table WHERE `时间` < from_unixtime(?)  and  `时间` > from_unixtime(?) ; "
    val startTime = LocalDateTime.of(2020, 4, 1, 0, 0, 0)
    val endTime = LocalDateTime.of(2019, 10, 1, 0, 0)
    val startTimeStamp = startTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli / 1000
    val endTimeStamp = endTime.toInstant(ZoneOffset.ofHours(8)).toEpochMilli / 1000

    //读取数据
    val jdbcRDD: JdbcRDD[(String, String, String, String, String)] = new JdbcRDD(
      sc,
      connection,
      selectSQL,
      startTimeStamp,
      endTimeStamp,
      2, //分区数目
      rs => {
        val name = rs.getString(1)
        val time = rs.getString(2)
        val result = rs.getString(3)
        val hosts = rs.getString(4)
        val game = rs.getString(5)
        (name, time, result, hosts, game)
      }
    )
    println(jdbcRDD.count())

    val JdbcDF = jdbcRDD.toDF("name", "time", "result", "hosts", "game") // toDF 方便匹配
    // 转化76->七六
    val mysqlDF = JdbcDF.withColumn("game", regexp_replace($"game", lit("76人"), lit("七六人")))

    // 拆解(湖人,102,快船,112)
    val dataDF = mysqlDF.withColumn("one", regexp_extract(split($"game", "-")(0), "[\\u4E00-\\u9FFF]+", 0))
      .withColumn("score1", regexp_extract(split($"game", "-")(0), "\\d+", 0))
      .withColumn("two", regexp_extract(split($"game", "-")(1), "[\\u4E00-\\u9FFF]+", 0))
      .withColumn("score2", regexp_extract(split($"game", "-")(1), "\\d+", 0))
    val rdd = dataDF.rdd
    val scoreRdd = rdd.map { row => (row.getString(5), row.getString(6).toLong, row.getString(7), row.getString(8).toLong) }

    // 注册累计器、使用
    val acc = new countAccumulator
    spark.sparkContext.register(acc)
    scoreRdd.collect().foreach(
      gameScore => {
        acc.add(gameScore._1, gameScore._2, gameScore._3, gameScore._4)
      }
    )

    // 取出累加器的值并排序打印
    val value = acc.value
    val dataList = value.map {
      case (categoryid, (win, sum)) => {
        (categoryid, win.toDouble / sum.toDouble)
      }
    }.toList

    dataList.sortWith {
      case (left, right) => {
        if (left._2 > right._2) {
          true
        } else {
          false
        }
      }
    }.foreach(println)

    sc.stop()
    spark.stop()
  }

}

//(湖人,102,快船,112)
class countAccumulator extends AccumulatorV2[(String, Long, String, Long), mutable.HashMap[String, (Long, Long)]] {
  private var map = mutable.HashMap[String, (Long, Long)]()
  private var list = (Long, Long)
  var longs: util.ArrayList[Long] = new util.ArrayList[Long]

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[(String, Long, String, Long), mutable.HashMap[String, (Long, Long)]] = new countAccumulator

  override def reset(): Unit = map.clear()

  override def add(v: (String, Long, String, Long)): Unit = {
    if (v._2 > v._4) {
      map(v._1) = (map.getOrElse(v._1, (0L, 0L))._1 + 1, map.getOrElse(v._1, (0L, 0L))._2 + 1)
      map(v._3) = (map.getOrElse(v._3, (0L, 0L))._1 + 0, map.getOrElse(v._3, (0L, 0L))._2 + 1)
    } else if (v._2 < v._4) {
      map(v._1) = (map.getOrElse(v._1, (0L, 0L))._1 + 0, map.getOrElse(v._1, (0L, 0L))._2 + 1)
      map(v._3) = (map.getOrElse(v._3, (0L, 0L))._1 + 1, map.getOrElse(v._3, (0L, 0L))._2 + 1)
    }
  }

  override def merge(other: AccumulatorV2[(String, Long, String, Long), mutable.HashMap[String, (Long, Long)]]): Unit = {
    val map1 = map
    val map2 = other.value
    map = map1.foldLeft(map2) {
      case (tempMap, (category, (winConut, sumCount))) => {
        tempMap(category) = (tempMap.getOrElse(category, (0L, 0L))._1 + winConut, tempMap.getOrElse(category, (0L, 0L))._2 + sumCount)
        tempMap
      }
    }
  }

  override def value: mutable.HashMap[String, (Long, Long)] = map
}
