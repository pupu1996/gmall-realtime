package cn.gp1996.gmall.publisher.controller;

import cn.gp1996.gmall.publisher.service.impl.DauServiceImpl;
import cn.gp1996.gmall.publisher.service.impl.OrderServiceImpl;
import cn.gp1996.gmall.utils.DateUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

/**
 * @author  gp1996
 * @date    2021-05-28
 * @desc
 * 注意：ArrayList -> jsonArray
 *      Map -> JsonObject
 */
@RestController
public class Controller {

    // dau服务
    @Autowired
    DauServiceImpl dauService;
    // gmv服务
    @Autowired
    OrderServiceImpl orderService;

    @RequestMapping("test")
    public String test() {
        return "success";
    }

    /**
     * 日活总计
     * @param date
     * @return
     */
    @RequestMapping("realtime-total")
    public String dauTotalByDate(@RequestParam("date") String date) {

        // 初始化最终的结果列表
        final ArrayList<Map<String, Object>> resList = new ArrayList<>();
        // 初始化日活统计信息map
        final Map<String, Object> dauInfoMap = new HashMap<>();
        // 初始化GMV统计map
        final HashMap<String, Object> gmvMap = new HashMap<>();
        // 初始化新增设备统计信息map
        final HashMap<String, Object> newMidMap = new HashMap<>();

        // 从Service获取每日的日活
        final long dau = dauService.getDauTotalByDate(date);

        // 从Service获取全日gmv
        final Double gmv = orderService.orderTotal(date);

        // 拼接每日日活的信息
        dauInfoMap.put("id", "dau");
        dauInfoMap.put("name", "新增日活");
        dauInfoMap.put("value", dau);

        // 拼接新增设备的信息
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);

        // 拼接GMV的统计信息
        gmvMap.put("id", "order_amount");
        gmvMap.put("name", "新增交易额");
        gmvMap.put("value", gmv);

        // 添加到最后结果列表中
        resList.add(dauInfoMap);
        resList.add(newMidMap);
        resList.add(gmvMap);

        final String jsonStr = JSONArray.toJSONString(resList);
        System.out.println(jsonStr);

        return jsonStr;
    }

    /**
     *
     * @param id 日活|gmv的tag
     * @param date
     * @return
     */
    @RequestMapping("realtime-hours")
    public String dauTotalPerHourByDate(
            @RequestParam("id") String id,
            @RequestParam("date") String date
    ) {

        // 获取前一日的日期
        final String yesterday = DateUtil.getLastDateStr(date);

        if ("dau".equals(id)) {
            // 初始化结果map(由前端可视化要求的数据格式确定)
            final HashMap<String, Map<String, Long>> resMap = new HashMap<>();
            // 调用service获取今日分时日活
            final Map<String, Long> todayMap = dauService.getDauTotalByDatePerHourMap(date);
            final Map<String, Long> yesterdayMap = dauService.getDauTotalByDatePerHourMap(yesterday);
            // 拼接结果
            resMap.put("yesterday", yesterdayMap);
            resMap.put("today", todayMap);
            return JSONObject.toJSONString(resMap);
        } else
        if ("order_amount".equals(id)) {
            // 初始化结果map(由前端可视化要求的数据格式确定)
            final HashMap<String, Map<String, Double>> resMap = new HashMap<>();
            final Map<String, Double> todayGMV = orderService.orderTotalHour(date);
            final Map<String, Double> yesterdayGMV = orderService.orderTotalHour(yesterday);
            resMap.put("yesterday", yesterdayGMV);
            resMap.put("today", todayGMV);
            return JSONObject.toJSONString(resMap);
        }

        return null;
    }
}
