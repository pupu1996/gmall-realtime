package cn.gp1996.gmall.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author  gp1996
 * @date    2021-05-28
 * @desc    读取.properties配置文件的工具类
 */
public class PropertiesUtil {

    // 保存该类的ClassLoader
    private static ClassLoader classloader;

    static {
        // 获取该类的classLoader
        classloader = PropertiesUtil.class.getClassLoader();
    }

    /**
     * 通过指定路径读取.properties文件
     * @param path
     * @return
     */
    public static Properties getProperties(String path) {

        // 读取配置文件，作为流返回
        final InputStream resourceAsStream = classloader.getResourceAsStream(path);

        // 创建新的Properties对象，将流加载进来
        Properties props = null;
        try {
            props = new Properties();
            props.load(resourceAsStream);
        }catch (IOException e) {
            e.printStackTrace();
        }

        return props;
    }

    // 测试
    public static void main(String[] args) {
        final Properties properties = PropertiesUtil.getProperties("config.properties");
        final String value = properties.getProperty("kafka.broker.list");
        System.out.println(value);
    }
}
