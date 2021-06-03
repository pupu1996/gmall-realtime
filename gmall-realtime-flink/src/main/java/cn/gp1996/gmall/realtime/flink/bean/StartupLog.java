package cn.gp1996.gmall.realtime.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;

/**
 * @author  gp1996
 * @date    2021-06-01
 * @desc    StartUpLog模板类
 */
@Data
public class StartupLog {
    private String mid;
    private String uid;
    private String appid;
    private String area;
    private String os;
    private String ch;
    private String type;
    private String vs;
    private String logDate;
    private String logHour;
    private Long ts;

    @Data
    @AllArgsConstructor
    public static class CntBloom {
        private Long cnt;
        private BloomFilter<String> bf;
    }
}
