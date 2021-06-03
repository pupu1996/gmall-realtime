package cn.gp1996.gmall.publisher.service;

import java.util.Map;

/**
 * @author  gp1996
 * @date    2021-05-29
 * @desc    需求1: 日活
 */
public interface DauService {
    // 获取日活数据
    long getDauTotalByDate(String date);

    // 根据date,获取当日分时统计Map
    Map<String, Long> getDauTotalByDatePerHourMap(String date);
}
