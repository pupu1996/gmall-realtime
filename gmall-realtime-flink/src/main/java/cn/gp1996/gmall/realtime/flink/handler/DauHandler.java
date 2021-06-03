package cn.gp1996.gmall.realtime.flink.handler;

import cn.gp1996.gmall.realtime.flink.bean.StartupLog;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;

/**
 * @author  gp1996
 * @date    2021-06-01
 * @desc    DauApp的自定义函数
 */
public class DauHandler {

    /**
     * 预聚合算子，使用BloomFilter
     * 布隆过滤器能百分百确定item是否存在，不存在就是不存在，对于存在的判断，存在一定的误判率
     */
    public static class DauWithBloomFilter
            implements AggregateFunction<StartupLog, StartupLog.CntBloom, Long> {

        // 初始化累加器
        @Override
        public StartupLog.CntBloom createAccumulator() {
            return new StartupLog.CntBloom(0L,
                    BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), 1000000, 0.01)
            );
        }

        // 每次数据到来的累加方法
        @Override
        public StartupLog.CntBloom add(StartupLog value, StartupLog.CntBloom accumulator) {
            // 获取Bloom过滤器
            final BloomFilter<String> bf = accumulator.getBf();
            // 判断新mid是否在bloom过滤器中
            final String newMid = value.getMid();
            final boolean isContain = bf.mightContain(newMid);
            if (!isContain) {
                // 如果mid没有在BloomFilter中
                // 计数加1
                accumulator.setCnt(accumulator.getCnt() + 1);
                // 更新布隆过滤器
                bf.put(newMid);
            }

            return accumulator;
        }

        // 获取最终结果
        @Override
        public Long getResult(StartupLog.CntBloom accumulator) {
            return accumulator.getCnt();
        }

        // 合并多个累加器的数据
        @Override
        public StartupLog.CntBloom merge(StartupLog.CntBloom a, StartupLog.CntBloom b) {
            return null;
        }
    }

    /**
     * 触发窗口时会调用的方法
     * 如果有进行预聚合，则elements中只有一条数据
     */
    public static class DauProcessWindowFunction
            extends ProcessWindowFunction<Long, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            final long start = context.window().getStart();
            final long end = context.window().getEnd();
            out.collect("window [" + start + "," + end + "] is triggered. cnt: " +elements.iterator().next());
        }
    }
}
