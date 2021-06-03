package cn.gp1996.gmall.utils;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.Date;

/**
 * @author  gp1996
 * @date    2021-05-30
 * @desc    时间日期相关工具类
 */
public class DateUtil {

    public static final String DATE_FORMAT_DAY ="yyyy-MM-dd";

    public static final String DATE_FORMAT_HOUR = "yyyy-MM-dd HH";

    public static final String DATE_FORMAT_MINUTE = "yyyy-MM-dd HH:mm";

    public static final String DATE_FORMAT_DATETIME = "yyyy-MM-dd HH:mm:ss";


    /**
     * 根据输入的时间格式获取指定日期
     * @param date
     * @param format
     * @return
     */
    public static String getDateByFormat(Date date, String format) {
        final SimpleDateFormat sdf = new SimpleDateFormat(format);
        final String res = sdf.format(date);
        return res;
    }

    /**
     * 由时间戳获取指定format的日期
     * @param timestamp
     * @param format
     * @return
     */
    public static String getDateByFormat(Long timestamp, String format) {
        return getDateByFormat(new Date(timestamp), format);
    }


    public static String getDateByFormat(String datetime, String format) {
        // 将dateStr -> date
        String resDate = null;
        final SimpleDateFormat sdf1 = new SimpleDateFormat(DATE_FORMAT_DATETIME);
        final SimpleDateFormat sdf2 = new SimpleDateFormat(format);
        try {
            Date tmpDate = sdf1.parse(datetime);
            resDate = sdf2.format(tmpDate);

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return resDate;
    }

    /**
     * 获取今天的日期
     * @return
     */
    public static String getToday() {
        return getDateByFormat(new Date(), DATE_FORMAT_DAY);
    }

    /**
     * 获取昨天的日期
     * @return
     */
    public static String getYesterday() {
        final Calendar calendar = Calendar.getInstance();
        // 设置日历基准时间
        calendar.setTime(new Date());
        // 操作时间(这里是天数-1)
        calendar.add(Calendar.DATE, -1);
        return getDateByFormat(calendar.getTime(), DATE_FORMAT_DAY);
    }

    public static String getLastDateStr(String date) {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String lastDate = null;
        try {
            Date res = sdf.parse(date);
            final Calendar calendar = Calendar.getInstance();
            calendar.setTime(res);
            calendar.add(Calendar.DATE, -1);
            lastDate = sdf.format(calendar.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return lastDate;
    }


    public static void main(String[] args) {
        System.out.println(getToday());
        System.out.println(getYesterday());

        final String lastDateStr = getLastDateStr("2021-05-30");
        System.out.println(lastDateStr);

        String dt = "2021-05-31 01:10:46";
        final String dh = getDateByFormat(dt, DATE_FORMAT_HOUR);
        System.out.println(dh);
    }

}
