package cn.gp1996.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author  gp1996
 * @date    2021-05-31
 * @desc    订单模型层
 */
public interface OrderMapper {

    // 全天GMV
    Double selectOrderAmountTotal(String date);

    // 当天GMV分词统计
    List<Map> selectOrderAmountTotalHour(String date);
}
