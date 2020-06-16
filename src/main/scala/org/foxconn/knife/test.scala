package org.foxconn.knife

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, max, min, pow, sqrt, sum}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import scala.collection.mutable.ListBuffer

object test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req1CategoryTop10Application")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._
    val feature = ListBuffer[knifeFeature]()
    sparkSession.sql("use" + "\t" + SparklUtil.getValueFromConfig("hive.database"))
    sparkSession.sql("SELECT id  FROM test group by id").collect().foreach(time => {
      // TODO 获取hive表中的数据
      // 时间戳
      val time_slot = time.getString(0).toFloat
      val sql = new StringBuilder("SELECT id,`date` ,value FROM  test where id  = ")
      sql.append(time_slot)
      val dataFrame = sparkSession.sql(sql.toString())
      val mapDF = dataFrame.map(row => (row.getString(0).toFloat, row.getString(1).toDouble, row.getString(2).toDouble))
      mapDF.cache()

      //平均值
      val average = mapDF.select(sum("_3") / count("_3")).rdd
      // 最小值
      val minValue = mapDF.select(min("_3")).rdd
      // 最大值
      val maxValue = mapDF.select(max("_3")).rdd
      // 均方根值
      val rootMeanSquare = mapDF.select(sqrt(sum(pow("_3", 2)) / count("_3"))).rdd
      // 联合形成list

      feature += knifeFeature(time_slot, average.take(1)(0).getDouble(0),
        minValue.take(1)(0).getDouble(0), maxValue.take(1)(0).getDouble(0),
        rootMeanSquare.take(1)(0).getDouble(0))
    })
    print(feature)
    sparkSession.close()

  }
}