package org.foxconn.knife

import java.io._
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


object fileTOKafka {
  def main(args: Array[String]): Unit = {
    val broker = SparklUtil.getValueFromConfig("kafka.broker.list")
    val topic = "X_Value"
    // 创建Kafka消费者
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // 根据配置创建Kafka生产者
    val kafkaProducer = new KafkaProducer[String, String](prop)

    val files = getFiles1(new File("G:\\201712181448-201712181458"))
    val files1 = files.sortWith {
      case (left, right) => {
        if (left.getName < right.getName) true
        else false
      }
    }
    files1.foreach(file => {
      // 读取文件
      try {
        println(file.toString)
        val reader = new BufferedReader(new FileReader(new File(file.toString)))
        var str = reader.readLine()

        while (str != null) {
          if (str.toString.charAt(0) == '1') {
            //            println(str)
            kafkaProducer.send(new ProducerRecord[String, String](topic, str))
            //            Thread.sleep(1000) 
          }
          str = reader.readLine()
        }


        reader.close()
        println(file.toString + "已经发送完毕")

      }
      catch {
        case e: Exception =>
          e.printStackTrace()
      }

    })
    kafkaProducer.close()
  }


  def getFiles1(dir: File): Array[File] = {
    dir.listFiles.filter(_.isFile) ++
      dir.listFiles.filter(_.isDirectory).flatMap(getFiles1)
  }

}