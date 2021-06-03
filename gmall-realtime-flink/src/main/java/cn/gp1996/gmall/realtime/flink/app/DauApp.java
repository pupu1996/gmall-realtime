package cn.gp1996.gmall.realtime.flink.app;

import cn.gp1996.gmall.constants.GmallConstants;
import cn.gp1996.gmall.realtime.flink.bean.StartupLog;
import cn.gp1996.gmall.realtime.flink.handler.DauHandler;
import cn.gp1996.gmall.utils.DateUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleDeserializers;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;
import java.util.regex.Pattern;

/**
 * 需求① 统计日活 fk
 * (1) 统计每天的dau
 * (2) 时间语义使用事件时间eventTime
 * (2) 使用滑动窗口，每5s统计一次日活
 * (3) 使用BloomFilter对(日期|mid)进行去重
 */
public class DauApp {
    public static void main(String[] args) {
        // 1.创建流执行环境
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间语义为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.从Kafka获取数据(Kafka connector)
        // 2.1 创建Kafka配置
        final Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty("bootstrap.servers", "hadoop111:9092,hadoop112:9092,hadoop113:9092");
        kafkaConsumerProps.setProperty("group.id", "gmall-startup-fink-test");
        kafkaConsumerProps.setProperty("auto.offset.reset", "latest");
        kafkaConsumerProps.setProperty("enable.auto.commit","false");

        // 2.2 创建flink kafka 消费者
        final FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                Pattern.compile("GMALL_STARTUP.*"),
                new SimpleStringSchema(),
                kafkaConsumerProps
        );

        final DataStreamSource<String> startLogStrStream = env.addSource(kafkaConsumer);

        final SingleOutputStreamOperator<StartupLog> startUpLogStream = startLogStrStream.map(new MapFunction<String, StartupLog>() {
            @Override
            public StartupLog map(String value) throws Exception {
                final StartupLog startupLog = JSONObject.parseObject(value, StartupLog.class);
                // 转换时间戳为时间字符串
                final String dateHour = DateUtil.getDateByFormat(startupLog.getTs(), DateUtil.DATE_FORMAT_HOUR);
                final String[] dateHourSplit = dateHour.split(" ");
                startupLog.setLogDate(dateHourSplit[0]);
                startupLog.setLogHour(dateHourSplit[1]);
                return startupLog;
            }
        }).assignTimestampsAndWatermarks(new WatermarkStrategy<StartupLog>() {
            @Override
            public WatermarkGenerator<StartupLog> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<StartupLog>() {

                    // 最大延时时间
                    private final long maxDelay = 2 * 1000L;

                    // 已到来事件的最大时间
                    private long maxEventTime = 0L;

                    @Override
                    public void onEvent(StartupLog event, long eventTimestamp, WatermarkOutput output) {
                        // 每到来一条数据更新最大事件时间
                        if (event.getTs() > maxEventTime) {
                            maxEventTime = event.getTs();
                        }

                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        // 周期性的触发wm
                        output.emitWatermark(new Watermark(maxEventTime - maxDelay));
                    }
                };
            }

            @Override
            public TimestampAssigner<StartupLog> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                return new TimestampAssigner<StartupLog>() {
                    @Override
                    public long extractTimestamp(StartupLog element, long recordTimestamp) {
                        return element.getTs();
                    }
                };
            }
        });

        final SingleOutputStreamOperator<String> uvStream = startUpLogStream.keyBy(
                new KeySelector<StartupLog, String>() {
                    @Override
                    public String getKey(StartupLog value) throws Exception {
                        return "key";
                    }
                }
        )
                .timeWindow(Time.hours(1L), Time.seconds(10L))
                .aggregate(new DauHandler.DauWithBloomFilter(), new DauHandler.DauProcessWindowFunction());
        // 输出转换结果
        // startUpLogStream.print();
        uvStream.print();


        // 执行job
        try {
            env.execute("Dau App");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
