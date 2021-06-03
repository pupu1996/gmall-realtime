package cn.gp1996.gmall.realtime.bean

/**
 * @author  gp1996
 * @desc    2021-06-02
 * @desc    优惠券领用报警日志
 * 注意：es不支持scala,传入es中的集合需要是java集合
 */
case class CouponAlertInfo(mid:String,
                           uids:java.util.HashSet[String],
                           itemIds:java.util.HashSet[String],
                           events:java.util.ArrayList[String],
                           ts:Long)
