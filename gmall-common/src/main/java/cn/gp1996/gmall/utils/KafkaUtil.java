package cn.gp1996.gmall.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author  gp1996
 * @date    2021-05-31
 * @desc    Kafka生产者单例
 *          Kafka生产者是线程安全的，允许多线程同时生产数据
 *          可以作为一个全局对象使用
 */
public class KafkaUtil {

    // 获取KafkaProperties配置
    private static final Properties kafkaProperties;

    static {
        kafkaProperties = PropertiesUtil.getProperties("kafka.properties");
    }

    // kafka生产者
    private static KafkaProducer<String, String> producer = null;

    // 返回Kafka实例
    public static KafkaProducer<String, String> getProducer(String topic
    ) {
        if (producer == null) {
            synchronized (KafkaUtil.class) {
                if (producer == null) {
                    producer = new KafkaProducer<String, String>(kafkaProperties);
                }
            }
        }

        return producer;
    }

    // 发送数据
    public static void send(String topic, String message) {
        // 获取Kafka实例
        final KafkaProducer<String, String> producer = getProducer(topic);
        producer.send(new ProducerRecord<String,String>(topic, message));
    }
}
