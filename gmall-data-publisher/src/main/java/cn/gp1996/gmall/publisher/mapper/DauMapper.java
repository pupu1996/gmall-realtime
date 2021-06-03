package cn.gp1996.gmall.publisher.mapper;

import java.util.List;
import java.util.Map;

/**
 * @author  gp1996
 * @date    2020-05-29
 * @desc    需求1:日活的操作层
 * 需要有和mapper配置同名的抽象方法
 * 实质：使用sql查询数据
 */
public interface DauMapper {
    // 日活总计
    Integer selectDauTotal(String date);
    // 分时统计
    List<Map> selectDauTotalHourMap(String date);
}

