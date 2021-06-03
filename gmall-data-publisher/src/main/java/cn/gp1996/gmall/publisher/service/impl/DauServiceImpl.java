package cn.gp1996.gmall.publisher.service.impl;

import cn.gp1996.gmall.publisher.mapper.DauMapper;
import cn.gp1996.gmall.publisher.service.DauService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author  gp1996
 * @date    2021-05-29
 * @desc    DauService实现类
 */
@Service
public class DauServiceImpl implements DauService {

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    DauMapper dauMapper;

    /**
     * 按照日期date,获取当日活统计值
     * @param date
     * @return
     */
    @Override
    public long getDauTotalByDate(String date) {
        return dauMapper.selectDauTotal(date);
    }

    /**
     * 按照日期date,获取日活的分时统计
     * @param date
     * @return
     */
    @Override
    public Map<String, Long> getDauTotalByDatePerHourMap(String date) {

        final List<Map> listPerHourCnt = dauMapper.selectDauTotalHourMap(date);
        // 初始化分时统计map map("11":236,"12":244...)
        final HashMap<String, Long> perHourCntMap = new HashMap<>();
        for (Map LHAndCT : listPerHourCnt) {
            final String hour = (String) LHAndCT.getOrDefault("LH", "null");
            final Long ct = (Long) LHAndCT.getOrDefault("CT", 0L);
            perHourCntMap.put(hour, ct);
        }

        return perHourCntMap;
    }
}
