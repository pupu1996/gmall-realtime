package cn.gp1996.gmall.publisher.service;

import cn.gp1996.gmall.publisher.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @author  gp1996
 * @date    2021-05-31
 * @desc    GMV服务
 */
public interface OrderService {

    // 全天GMV
    public Double orderTotal(String date);

    // GMV分时统计
    public Map<String, Double> orderTotalHour(String date);
}
