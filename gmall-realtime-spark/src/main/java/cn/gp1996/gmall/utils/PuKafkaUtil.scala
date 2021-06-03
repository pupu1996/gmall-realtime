package cn.gp1996.gmall.utils

import cn.gp1996.gmall.constants.GmallConstants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}

import java.util.Properties

/**
 * @author  gp1996
 * @date    2021-06-01
 * @desc    SparkStreaming整合Kafka工具类
 */
object PuKafkaUtil {
  def createStream(topic: String, ssc: StreamingContext): DStream[ConsumerRecord[String, String]] = {
    // 1.从配置文件中读取kafka的配置
//    val kafkaConsumerProperties: Properties =
//      PropertiesUtil.getProperties(GmallConstants.GMALL_CONF_PATH)
val kafkaParams: Map[String, Object] = Map[String, Object](
  "bootstrap.servers" -> "hadoop111:9092,hadoop112:9092,hadoop113:9092",
  "key.deserializer" -> classOf[StringDeserializer],
  "value.deserializer" -> classOf[StringDeserializer],
  "group.id" -> "gmv-cgroup-test",
  "auto.offset.reset" -> "latest",
  "enable.auto.commit" -> (false: java.lang.Boolean)
)
    // 2.使用Kafka工具创建数据流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](Seq(topic), kafkaParams)
    )
    kafkaDStream
  }
}
