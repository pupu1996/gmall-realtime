package cn.gp1996.gmall.realtime.app

import cn.gp1996.gmall.constants.GmallConstants
import cn.gp1996.gmall.realtime.bean.{CouponAlertInfo, EventLog}
import cn.gp1996.gmall.utils.{DateUtil, PuESUtil}
import com.alibaba.fastjson.JSON
import io.searchbox.client.JestClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import java.{lang, util}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/**
 * @author gp1996
 * @date   2021-06-02
 * @desc   需求三
 *
 * 下游需要用户浏览商品行为日志 + 预警日志
 * 都包括在疑似日志中
 * 通过uids.size>=3且bool来判断属于哪种日志
 * bool代表用户是否浏览过商品
 *
 */
object CouponAlertApp {
  def main(args: Array[String]): Unit = {
    // 1.创建spark配置
    val conf = new SparkConf()
      .setAppName("CouponAlertApp")
      .setMaster("local[*]")
    // 2.创建流上下文(一批的时间3s)
    val ssc = new StreamingContext(conf, batchDuration = Seconds(5))
    // 创建kafka配置
    val kafkaParam: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop111:9092,hadoop112:9092,hadoop113:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "coupon-alter-app-cgroup-test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )
    // 使用KafkaUtils创建流
    val eventLogStrDStream: InputDStream[ConsumerRecord[String,String]] =
      KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      Subscribe[String, String](
        Seq(GmallConstants.KAFKA_TOPIC_EVENT),
        kafkaParam)
    )
    // 转换成样例类类型
    val eventLogStream: DStream[(String,EventLog)] = eventLogStrDStream.mapPartitions {
      case logs => {
        logs.map {
          case log => {
            val eventLog: EventLog = JSON.parseObject(log.value(), classOf[EventLog])
            val dateHour: String = DateUtil.getDateByFormat(eventLog.ts, DateUtil.DATE_FORMAT_HOUR)
            val dateHourSplit: Array[String] = dateHour.split(" ")
            eventLog.logDate = dateHourSplit(0)
            eventLog.logHour = dateHourSplit(1)
            (eventLog.mid, eventLog)
          }
        }
      }
    }

    // eventLogStream.print()
    // 先进行window，再进行分组
    // 先对窗口内所有RDD先做union,然后再进行groupBy去重
    val groupByMidDStream: DStream[(String, Iterable[EventLog])] = eventLogStream
      .window(Minutes(5))
      .groupByKey()

    // 判断mid每条日志
    // 判断领取优惠券行为,使用标志位判断是否有浏览商品，默认为true，
    // 如果有，标志位为false
    // 筛选出疑似日志
//    groupByMidDStream.foreachRDD(
//      rdd => {
//        rdd.foreachPartition{ // 每个partition(Task线程)创建一个连接
//          case iter => {
//            iter.foreach{
//              case (mid,logs) => {
//                // 初始化uid集合
//                val uids = mutable.Set.empty[String]
//                // 初始化优惠券关联商品set
//                val itemIds = mutable.Set.empty[String]
//                // 初始化行为list
//                val events: ListBuffer[String] = mutable.ListBuffer.empty[String]
//                // 初始化bool
//                var noBrowser = true
//
//                breakable{
//                    for (log <- logs.iterator) {
//                      events ++ log.evid
//                      if ("coupon".equals(log.evid)) {
//                        // TODO 如果领取消费券，写入uids,itemsId
//                        uids ++ log.uid
//                        itemIds ++ log.itemid
//                      } else if ("clickItem".equals(log.evid)) {
//                        // TODO 如果有浏览页面的情况，直接退出
//                        noBrowser = false
//                        break()
//                      }
//                    }
//                  }
//
//                // 判断
//                if (uids.size >=3 && noBrowser) {
//                  // 确实为预警日志,写入redis
//                }
//
//              }
//            }
//          }
//        }
//      }
//    )
    // 过滤出疑似日志
    val alertSuspectedDStream: DStream[(Boolean, CouponAlertInfo)] = groupByMidDStream.mapPartitions(
      iter => { // iter多台mid
        iter.map { // 返回多台mid的疑似日志
          case (mid, logs) => {
            // 初始化uid集合
            val uids = new util.HashSet[String]()
            // 初始化优惠券关联商品set
            val itemIds = new util.HashSet[String]()
            // 初始化行为list
            val events = new java.util.ArrayList[String]()
            // 初始化bool
            var noBrowser = true

            breakable {
              for (log <- logs.iterator) {
                events.add(log.evid)
                if ("coupon".equals(log.evid)) {
                  // TODO 行为为领取购物券，添加数据到预警日志中(疑似预警)
                  uids.add(log.uid)
                  itemIds.add(log.itemid)

                } else if ("clickItem".equals(log.evid)) {
                  // TODO 不满足预警，直接退出
                  noBrowser = false
                  break()
                }
              }
            }

            // 封装疑似报警日志(uids.size >= 3 && noBrowser =1 警告, =0 非警告)
            (uids.size >= 1 && noBrowser, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
          }
        }
      }
    )

    // 过滤出真正的警告日志
    alertSuspectedDStream
      .filter(_._1)
      .repartition(1)
      .foreachRDD(
        rdd => {
          rdd.foreachPartition{
            case alertLogs => {
              val client: JestClient = PuESUtil.getClient
              alertLogs.foreach{
                case alertLog => {
                  // 构建docid
                  val ts: Long = alertLog._2.ts
                  val dateItem: String = DateUtil.getDateByFormat(ts, DateUtil.DATE_FORMAT_MINUTE)
                  val docId: String = alertLog._2.mid + dateItem

                  PuESUtil.insert(client, alertLog._2, "coupon_alert_index_test", docId)
                }
              }
              PuESUtil.closeClient(client)
            }
          }
        }
      )

    // *.启动上下文
    ssc.start()
    // *.阻塞主线程
    ssc.awaitTermination()
  }
}
