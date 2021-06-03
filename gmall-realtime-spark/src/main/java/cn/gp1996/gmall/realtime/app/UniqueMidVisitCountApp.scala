package cn.gp1996.gmall.realtime.app

import cn.gp1996.gmall.constants.GmallConstants
import cn.gp1996.gmall.realtime.bean.StartUpLog
import cn.gp1996.gmall.realtime.handler.UniqueMidVisitCountHandler
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat

/**
 * @author gp1996
 * @date 2021-05-28
 * @desc 需求：统计访客日活
 */
object UniqueMidVisitCountApp {
  def main(args: Array[String]): Unit = {
    // 1.创建spark配置
    val conf = new SparkConf()
      .setAppName("dau_app")
      .setMaster("local[*]")
    // 2.创建sparkStreaming上下文
    val ssc = new StreamingContext(conf, batchDuration = Seconds(5))

    // 3.业务处理
    // 3.1 使用Kafka工具类从Kafka中消费数据
    // http://spark.apache.org/docs/2.4.0/streaming-kafka-0-10-integration.html

    // 3.1.1 创建Kafka Consumer 配置
    val kafkaConsumerConf = Map[String, Object](
      "bootstrap.servers" -> "hadoop111:9092,hadoop112:9092,hadoop113:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "start-log-cgroup-test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 3.1.2 从kafka读取数据
    val startLogKDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc, // sparkStreaming上下文
      PreferConsistent, // 优先存储
      Subscribe[String, String](Seq(GmallConstants.KAFKA_TOPIC_STARTUP), kafkaConsumerConf)
    )
    // 3.2 将启动日志(json)转换为启动日志模板类，并解析时间和小时
    val startLogDStream: DStream[StartUpLog] = startLogKDStream
      .mapPartitions(
        startLogs => {
          startLogs.map {
            case record => {
              val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
              val startupLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
              val date_hour: Array[String] = sdf.format(startupLog.ts).split(" ")
              startupLog.logDate = date_hour(0)
              startupLog.logHour = date_hour(1)
              startupLog
            }
          }
        }
      )

    // 3.3 使用Redis进行批次间去重(forRDD,forPartitions)
    //val filterByMidDStream: DStream[StartUpLog] = UniqueMidVisitCountHandler
    //  .filterByMidPlanB(startLogDStream, ssc)
    // 分区间过滤(方案3)
    val filterByMidDStream: DStream[StartUpLog] =
    UniqueMidVisitCountHandler.filterByMidPlanC(startLogDStream, ssc)

    // 3.4 每日的日活用户存入redis(在去重的时候已经做了,方案3需要在这里做)
    val updatedMidDStream: DStream[StartUpLog] = UniqueMidVisitCountHandler.updateMidsToRedis(filterByMidDStream)

    // 3.5 groupByKey进行分区内去重，然后统计
    // 分区内过滤(根据map)
    val filterByGroupByDStream: DStream[StartUpLog] =
      UniqueMidVisitCountHandler.filterByGroupBy(updatedMidDStream)

    // 3.7 明细数据存入hbase(幂等性去重，保存一个mid访问明细，versions = 1?)
    UniqueMidVisitCountHandler.writeDetail2HBase(filterByGroupByDStream)

    // 4.启动应用
    ssc.start();
    // 5.阻塞主线程
    ssc.awaitTermination();
  }
}
