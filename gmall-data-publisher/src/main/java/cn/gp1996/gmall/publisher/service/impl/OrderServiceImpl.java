package cn.gp1996.gmall.publisher.service.impl;

import cn.gp1996.gmall.publisher.mapper.OrderMapper;
import cn.gp1996.gmall.publisher.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class OrderServiceImpl implements OrderService {

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private OrderMapper orderMapper;


    // 全天GMV
    @Override
    public Double orderTotal(String date) {
        // 调用Mapper获取数据
        return orderMapper.selectOrderAmountTotal(date);
    }

    // 当天GMV分时统计
    @Override
    public Map<String, Double> orderTotalHour(String date) {
        // 初始化结果map
        final HashMap<String, Double> hourAmountMap = new HashMap<>();

        // 从mapper获取gmv分时统计数据
        final List<Map> kvMapList = orderMapper.selectOrderAmountTotalHour(date);
        for (Map kvMap : kvMapList) {
            hourAmountMap.put(
                    (String)kvMap.getOrDefault("CH", ""),
                    (Double)kvMap.getOrDefault("TA", 0.0)
            );
        }

        return hourAmountMap;
    }
}
