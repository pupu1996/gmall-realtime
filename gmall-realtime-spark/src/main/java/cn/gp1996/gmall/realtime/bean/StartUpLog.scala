package cn.gp1996.gmall.realtime.bean

/**
 * @author gp1996
 * @date 2021-05-28
 * @desc 用户启动样例类
 */
case class StartUpLog(mid:String,
                      uid:String,
                      appid:String,
                      area:String,
                      os:String,
                      ch:String,
                      `type`:String,
                      vs:String,
                      var logDate:String,
                      var logHour:String,
                      var ts:Long)
