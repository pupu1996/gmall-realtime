package cn.gp1996.gmall.constants;

public class GmallConstants {

    /** -----------------------------------------------------------
     * -------------------- Conf -------------------------
     * -----------------------------------------------------------
     */

    // 项目配置文件路径
    public static final String GMALL_CONF_PATH = "config.properties";

    // redis配置参数
    public static final String CONF_REDIS_HOST = "redis.host";
    public static final String CONF_REDIS_PORT = "redis.port";


    /** -----------------------------------------------------------
     * -------------------- Kafka Topics -------------------------
     * -----------------------------------------------------------
     */
    // 启动数据主题
    public static final String KAFKA_TOPIC_STARTUP = "GMALL_STARTUP";
    // 订单主题
    public static final String KAFKA_TOPIC_ORDER = "GMALL_ORDER_INFO";
    // 事件主题
    public static final String KAFKA_TOPIC_EVENT = "GMALL_EVENT";

    /** -----------------------------------------------------------
     * -------------------- 实时需求前缀 -------------------------
     * -----------------------------------------------------------
     */
    // 项目名
    public static final String APP_NAME = "GMALL";
    // 日活
    public static final String PREFIX_DAU = "dau";

}
