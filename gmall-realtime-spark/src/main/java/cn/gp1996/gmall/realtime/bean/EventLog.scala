package cn.gp1996.gmall.realtime.bean

/**
 * @author gp1996
 * @date   2021-06-02
 * @desc   事件日志样例类
 */
case class EventLog(
         `type`: String,
         mid: String,
         uid: String,
         os: String,
         appid: String,
         area: String,
         evid: String,
         pgid: String,
         npgid: String,
         itemid: String,
         var logDate: String,
         var logHour: String,
         ts: Long
       )

// evid
// mid
// uid