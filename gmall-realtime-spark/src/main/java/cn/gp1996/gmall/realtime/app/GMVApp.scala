package cn.gp1996.gmall.realtime.app

import cn.gp1996.gmall.constants.GmallConstants
import cn.gp1996.gmall.realtime.bean.OrderInfo
import cn.gp1996.gmall.utils.DateUtil
import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

import java.util.Date
import scala.util.parsing.json.JSONObject
/**
 * @author  gp1996
 * @date    2020-05-31
 * @desc
 */
object GMVApp {

  def main(args: Array[String]): Unit = {
    // 1.创建配置
    val conf = new SparkConf()
      .setAppName("GMVApp")
      .setMaster("local[*]")

    // 2.创建SparkStreaming上下文
    val ssc = new StreamingContext(conf, batchDuration = Seconds(5))

    // 3.创建Kafka消费者配置
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop111:9092,hadoop112:9092,hadoop113:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "gmv-cgroup-test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 4.从Kafka创建DStream
    val orderDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Seq(GmallConstants.KAFKA_TOPIC_ORDER), kafkaParams)
    )

    // 5.转化为模板类
    val orderInfoDStream: DStream[OrderInfo] = orderDStream.mapPartitions(
      iter => iter.map(
        log => {
          val orderInfoStr: String = log.value()
          val orderInfo: OrderInfo = JSON.parseObject(orderInfoStr, classOf[OrderInfo])
          // 获取订单创建时间yyyy-MM-dd HH:mm:ss
          val ct: String = orderInfo.create_time
          val dateHour: String = DateUtil.getDateByFormat(ct, DateUtil.DATE_FORMAT_HOUR)
          val dateHourSplit: Array[String] = dateHour.split(" ")
          orderInfo.create_date = dateHourSplit(0)
          orderInfo.create_hour = dateHourSplit(1)
          orderInfo
        }
      )
    )

    // 6.存入数据到Hbase中
    orderInfoDStream.foreachRDD(
      rdd => {
        rdd.saveToPhoenix(
          GmallConstants.KAFKA_TOPIC_ORDER,
          Seq("ID","PROVINCE_ID","CONSIGNEE","ORDER_COMMENT",
            "CONSIGNEE_TEL","ORDER_STATUS","PAYMENT_WAY",
            "USER_ID","IMG_URL","TOTAL_AMOUNT",
            "EXPIRE_TIME","DELIVERY_ADDRESS",
            "CREATE_TIME","OPERATE_TIME","TRACKING_NO",
            "PARENT_ORDER_ID","OUT_TRADE_NO","TRADE_BODY",
            "CREATE_DATE","CREATE_HOUR"
          ),
          HBaseConfiguration.create(),
          Some("hadoop111,hadoop112,hadoop113:2181")
        )
      }
    )

    // *.启动ssc
    ssc.start()
    // *.阻塞主线程
    ssc.awaitTermination()
  }
}
