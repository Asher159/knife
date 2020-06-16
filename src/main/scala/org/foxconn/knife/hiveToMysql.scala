package org.foxconn.knife

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.sqrt
import org.apache.spark.sql.functions.pow
import scala.collection.mutable.ListBuffer

object hiveToMysql {
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
      val maxValue = mapDF.select(max("_3") ).rdd
      // 均方根值
      val rootMeanSquare = mapDF.select(sqrt(sum(pow("_3", 2)) / count("_3"))).rdd
      // 联合形成list

      feature += knifeFeature(time_slot, average.take(1)(0).getDouble(0),
        minValue.take(1)(0).getDouble(0), maxValue.take(1)(0).getDouble(0),
        rootMeanSquare.take(1)(0).getDouble(0))
      print(feature)
    })
    //    todo sparkSQL保存
    feature.toDF().write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/nba?useSSL=false&serverTimezone=Asia/Shanghai")
      .option("dbtable", "knife_feature")
      .option("user", "root")
      .option("password", "123456")
      .mode("Append")
      .save()


    //    // TODO  将结果保存到Mysql中 Java jdbc 保存
    //    val driverClass = SparklUtil.getValueFromConfig("jdbc.driver.class")
    //    val url = SparklUtil.getValueFromConfig("jdbc.url")
    //    val user = SparklUtil.getValueFromConfig("jdbc.user")
    //    val password = SparklUtil.getValueFromConfig("jdbc.password")
    //    Class.forName(driverClass)
    //
    //    val connection: Connection = DriverManager.getConnection(url, user, password)
    //    val insertSQL = "insert into knife_feature(time_slot,average_value," +
    //      " min_value,max_value,root_mean_square) values ( ?, ?, ?, ?, ? )"
    //    val statement: PreparedStatement = connection.prepareStatement(insertSQL)
    //    feature.toList.foreach(data => {
    //      statement.setObject(1, data.time_slot)
    //      statement.setObject(2, data.average_value)
    //      statement.setObject(3, data.min_value)
    //      statement.setObject(4, data.max_value)
    //      statement.setObject(5, data.root_mean_square)
    //      statement.executeUpdate()
    //    })
    //    statement.close()
    //    connection.close()


    sparkSession.stop()
  }
}

case class knifeFeature(time_slot: Float, average_value: Double, min_value: Double, max_value: Double, root_mean_square: Double)