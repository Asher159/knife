package org.foxconn.knife

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable.ListBuffer

object kafkaTOHive {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafkaTOHive").setMaster("local[*]")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "1") //每次拉取5条
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //enableHiveSupport() 打开hive支持，spark会默认去读取配置文件Hive-site.xml中的配置
    //这里先生成sparkSession ，然后生成sc，在生成ssc,这用就能保证sparkSession、sc、ssc三者共存，不然会报多个sc异常错误
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5)) // 有区别于val ssc2 = new StreamingContext(sparkConf,Seconds(5))


    // TODO 4.1 从kafka中周期性获取数据
    val topic = "flume-data"
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

    val realData: DStream[dataAction] = kafkaDStream.map(record => {
      val message: String = record.value()
      val datas: Array[String] = message.split("\t")
      dataAction(datas(0).charAt(3).toString, datas(0), datas(1))
    })

    realData.foreachRDD(rdd => {
      import sparkSession.implicits._
      val DFdata = rdd.toDF()
      //如需要把数据分区分割并保存文件，一种思路是先将一批数据保存到相应路径,然后在通过命令修复分区表，另一中思路是使用insertHive2中的方式
      //load data local inpath '/opt/module/datas/dept.txt' into tabledept_partition2 partition(month='201709',day='10');
      insertHive(sparkSession, "test", DFdata) // 执行保存
      //      if(rdd.count()<5){rdd.collect().foreach(println)}
      //      rdd.collect().foreach(println)
    })
    ssc.start()
    ssc.awaitTermination()

  }


  def insertHive(sparkSession: SparkSession, tableName: String, dataFrame: DataFrame): Unit = {
    sparkSession.sql("use " + SparklUtil.getValueFromConfig("hive.database"))
    //    sparkSession.sql("drop table if exists " + tableName)
    dataFrame.write.saveAsTable(tableName)
    println("保存：" + tableName + "完成")
    sparkSession.sql("select * from " + tableName).show(3)
  }

//TODo 这里定义了一种sparkSQL自带的插入方式，支持插入分区表，追加数据等操作。按需使用
  def insertHive2(sparkSession: SparkSession, dataFrame: DataFrame): Unit ={
    dataFrame.toDF().createOrReplaceTempView("addhive")
    sparkSession.sql("insert into table2 partition(date='2015-04-03') select * from addhive")
  }
}


object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()
    }
    instance
  }
}

case class dataAction(id: String, date: String, value: String)