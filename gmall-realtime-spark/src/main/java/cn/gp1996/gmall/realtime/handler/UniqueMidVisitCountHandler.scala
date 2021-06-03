package cn.gp1996.gmall.realtime.handler

import cn.gp1996.gmall.constants.GmallConstants
import cn.gp1996.gmall.realtime.bean.StartUpLog
import cn.gp1996.gmall.utils.PropertiesUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.phoenix.spark._
import redis.clients.jedis.Jedis

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

/**
 * @author gp1996
 * @date 2021-05-28
 * @desc 设备日活需求UniqueMidVisitCountApp 的Handler
 */
object UniqueMidVisitCountHandler {

  private val props: Properties = PropertiesUtil.getProperties(GmallConstants.GMALL_CONF_PATH)

  /**
   * 使用Redis保存已经存在的mid, 新批次的数据运行时会对比redis中保存的历史数据，进行去重
   * 有三种方案：
   * 方案① 每处理一条数据创建一条连接 （×）
   * 方案② 在每一个Partition中创建一个Redis连接
   * 方案③ 在一个批次中创建一redis连接
   *
   * 这个方法使用第②种方案
   *
   * @param stream
   * @param ssc
   */
  def filterByMidPlanB(startLogDStream: DStream[StartUpLog], ssc: StreamingContext) = {
    // 获取redis相关的配置信息
    val redisHost: String = props.getProperty(GmallConstants.CONF_REDIS_HOST)
    val redisPort: Int = props.getProperty(GmallConstants.CONF_REDIS_PORT).toInt

    // 以RDD的分区为单位进行处理(需要返回值)
    val filterByMidDSteam: DStream[StartUpLog] = startLogDStream.mapPartitions {
      case startLogs => { // 对于每个分区 => 就是每个Task线程内
        // 新建redis连接客户端
        val jedis = new Jedis(redisHost, redisPort)
        // 创建simpleDateFormat
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        // 遍历每条数据
        val filterWithMidLogs: Iterator[StartUpLog] = startLogs.filter(
          startLog => {
            // 获取keyName
            val keyName = GmallConstants.PREFIX_DAU + sdf.format(new Date(System.currentTimeMillis()))
            val isRepeat: lang.Boolean = jedis.sismember(
              keyName,
              startLog.mid
            )

            // 如果是新的mid，添加到redis的集合中
            if (!isRepeat) jedis.sadd(keyName, startLog.mid)
            !isRepeat // 保留非命中的
          }
        )
        // 关闭redis连接
        jedis.close()
        // 返回分区间过滤后的数据流
        filterWithMidLogs
      }
    }

    filterByMidDSteam

  }

  /**
   * 方案③
   * 整个批次创建一个redis连接
   * 使用foreachRDD
   * 1. 在Driver端创建redis连接,获取历史的mid数据
   * 2. 将历史的mid数据包装为广播变量发送到每个Executor中
   * 3. 调用mapPartitions,遍历每个Partition内的数据,获取广播变量的值(每个ExecutorBackend一份广播变量的副本)
   * 4. 获取历史mids,进行去重
   *
   *
   * @param dStream
   * @param ssc
   */
  def filterByMidPlanC(stream: DStream[StartUpLog], ssc: StreamingContext) = {

    // 使用transform算子，在每个批次创建一个连接(每个批次在Driver执行一次)
    stream.transform(
      startLogRDD => {

        // 读取配置文件
        val host: String = props.getProperty(GmallConstants.CONF_REDIS_HOST)
        val port: Int = props.getProperty(GmallConstants.CONF_REDIS_PORT).toInt

        // 1.创建jedis连接
        val jedis = new Jedis(
          host,
          port
        )

        val sdf = new SimpleDateFormat("yyyy-MM-dd")

        // 2.获取历史mids
        val historyMids: util.Set[String] = jedis.smembers(GmallConstants.PREFIX_DAU + sdf.format(new Date(System.currentTimeMillis())))

        // 3.将历史mids包装为广播变量
        val bcMids: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast[util.Set[String]](historyMids)

        // 从jedis中获取mid的历史数据(真正的时候要获取前后两天的mids,避免跨天的批次，出现错误)
        val filterByMidStartLogRDD: RDD[StartUpLog] = startLogRDD.filter {
          case startLog => {
            // 从广播的历史数据中，判断是否存在重复的
            val isRepeat: Boolean = bcMids.value.contains(startLog.mid)
            !isRepeat
          }
        }

        // 所有数据都处理完后，将更新的mid存入redis中
        filterByMidStartLogRDD

        // 关闭redis连接
        jedis.close()
        filterByMidStartLogRDD
      }
    )
  }

  /**
   * 去重后的数据加入Redis中(针对方案3)
   * 不需要返回值
   * @param stream
   */
  def updateMidsToRedis(stream: DStream[StartUpLog]) = {
    stream.transform(
      rdd => {
        // 在分区内创建连接
        rdd.foreachPartition(
          logs => {
            // 创建redis连接
            val jedis = new Jedis(
              props.getProperty(GmallConstants.CONF_REDIS_HOST),
              props.getProperty(GmallConstants.CONF_REDIS_PORT).toInt
            )
            // 遍历iter,根据事件时间ts,将数据放入对应的日期内
            logs.foreach(
              log => jedis.sadd(GmallConstants.PREFIX_DAU + ":" + log.logDate, log.mid)
            )
            jedis.close()
          }
        )
        rdd
      }
    )
  }


  /**
   * 进行分区内的去重
   * 做法：groupBy将相同的(mid,ts)分为一组，对同一个mid的访问记录按照ts升序排序，
   * 返回其中的第一条数据(即ts最小的记录)
   * @param stream
   */
  def filterByGroupBy(stream: DStream[StartUpLog]) = {
      // 需要返回dstream
      stream.transform(
        startLogRDD => {
          startLogRDD
            .groupBy(log => (log.mid, log.logDate))
            .values
            .mapPartitions{
              case iter =>
                iter.map(
                  _.toList.sortWith(_.ts < _.ts).take(1)
                )
            }.flatMap(x => x)
        }
      )
  }

  /**
   * 将用户登入明细数据存入Hbase中
   * 1.使用phoenix创建业务表
   * 2.导入spark-phoenix的依赖
   * 3.使用rdd.saveToPhoenix(
   *  表名,
   *  字段名列表,
   *  conf,
   *  zkUrl,
   *  ...
   * )
   * @param stream
   */
  def writeDetail2HBase(stream: DStream[StartUpLog]) = {
    stream.foreachRDD(
      rdd => {
        // 这里只有rdd的算子是在executor端运行的,
        // 其他在Driver端运行
        rdd.saveToPhoenix(
          GmallConstants.APP_NAME + "_" + GmallConstants.PREFIX_DAU.toUpperCase,
          Seq("MID","UID","APPID","AREA","OS","CH","TYPE","VS","LOGDATE","LOGHOUR","TS"),
          HBaseConfiguration.create(),
          Some("hadoop111,hadoop112,hadoop113:2181")
        )
      }
    )
  }
}
