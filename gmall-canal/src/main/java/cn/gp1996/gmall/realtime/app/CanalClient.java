package cn.gp1996.gmall.realtime.app;

import cn.gp1996.gmall.constants.GmallConstants;
import cn.gp1996.gmall.utils.KafkaUtil;
import cn.gp1996.gmall.utils.PropertiesUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sun.xml.internal.bind.v2.TODO;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.net.InetSocketAddress;
import java.util.*;

/**
 * @author  gp1996
 * @date    2021-06-03
 * @desc
 * 处理变更数据
 *        --------------- 需求三 --------------------
 * 需要是: (1) 表名是order_info 数据EventType为insert
 *        --------------- 需求四添加 -----------------
 *        (2) 表名是order_detail 数据EventType为insert
 *        (3) 表名是user_info 数据EventType为insert|update
 */
public class CanalClient {

    // 保存Canal配置文件的内容
    private static Properties canalProperties = null;
    // 监控表的前缀
    private static final String PREFIX_CANAL_MONITOR_TABLE = "canal.monitor.table";
    private static final String SUFFIX_CANAL_MONITOR_TABLE_TYPE = "types";

    // 保存监控的table的map
    private static Map<String, List<CanalEntry.EventType>> monitorMap;

    static {
        // 加载Canal配置文件的内容
        canalProperties = PropertiesUtil.getProperties("canal.properties");
        monitorMap = resolveMonitorProperties();
    }

    // private static final String ORDER_TABLE = "order_info";

//    public static void main(String[] args) {
//        // 1.创建Canal连接器(工厂，单连接器)
//        final CanalConnector canalConnector = CanalConnectors.newSingleConnector(
//                new InetSocketAddress("hadoop111", 11111),
//                "example", "", ""
//        );
//
//        // 2.获取连接
//        canalConnector.connect();
//
//        // 3.指明要监控的数据库
//        canalConnector.subscribe(canalProperties.getProperty("canal.monitor.databases"));
//
//        try{
//            while (true) {
//                // 拉取数据Message
//                // 100是一次拉取的entry数量
//                final Message message = canalConnector.get(100);
//                // 获取entry
//                final List<CanalEntry.Entry> entries = message.getEntries();
//                if (entries.size() <= 0) {
//                    System.out.println("没有数据，休息一会儿");
//                    Thread.sleep(5 * 1000L);
//                } else {
//                    // 如果有新增和更新的数据了，解析entry
//                    for (CanalEntry.Entry entry : entries) {
//                        // 获取表名
//                        final String tblName = entry.getHeader().getTableName();
//                        // 获取entry类型
//                        final CanalEntry.EntryType entryType = entry.getEntryType();
//                        // 判断entryType的类型是否为ROWDATA(实际的数据行)
//                        // entry中包含了多条受影响的行数据
//                        if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
//                            // 反序列化 ->
//                            final CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
//                            // 统计订单数据 -> 只需统计第一次下单行为 -> EventType = INSERT
//                            // 获取EntryType(一条sql一个EntryType多条受影响的行)
//                            final CanalEntry.EventType eventType = rowChange.getEventType();
//                            // 获取行数据列表
//                            final List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
//                            handleChange(tblName, eventType, rowDatasList);
//                        }
//                    }
//                }
//            }
//        } catch (InterruptedException | InvalidProtocolBufferException e) {
//            e.printStackTrace();
//        } finally {
//            canalConnector.disconnect();
//        }
//
//    }

    /**
     * 处理变更数据
     *        --------------- 需求三 --------------------
     * 需要是: (1) 表名是order_info 数据EventType为insert
     *        --------------- 需求四添加 -----------------
     *        (2) 表名是order_detail 数据EventType为insert
     *        (3) 表名是user_info 数据EventType为insert|update
     * @param tblName
     * @param et
     * @param rows
     */
    private static void handleChange(
            String tblName, CanalEntry.EventType et, List<CanalEntry.RowData> rows) {

//        // 需要匹配三种情况
//
//        // 初始化结果map arrayList -> jsonArray ; map -> jsonObject
//        final HashMap<String, String> rowMap = new HashMap<String, String>();
//        System.out.println("新增插入数据 " + rows.size() + " 条");
//        // 遍历每一条sql影响的行
//        for (CanalEntry.RowData row : rows) {
//            // 解析RowData数据结构
//            for (CanalEntry.Column column : row.getAfterColumnsList()) {
//                rowMap.put(column.getName(), column.getValue());
//            }
//
//            // 发送数据到Kafka
//            KafkaUtil.send(GmallConstants.KAFKA_TOPIC_ORDER, JSONObject.toJSONString(rowMap));
//            // 清空map
//            rowMap.clear();
//        }
        // TODO 1.判断该类日志是否需要(通过 表名+行为类型 来判断)
        // TODO 2.遍历rowChangeList
        // TODO 3.解析行数据,写入Kafka中

    }

    /**
     * 判断是否属于需要监控的日志范围
     * @param tableName
     * @param ets
     * @return
     */
    public static boolean isRowNeeded(String tableName, CanalEntry.EventType ets) {
        // TODO 判断tableName表对应的EventType数据是否要被收集
        // 判断tableName是否在monitorMap中
        if (monitorMap.containsKey(tableName)) {
            // tableName存在，拿出监听的事件类型列表
            final List<CanalEntry.EventType> eventTypes = monitorMap.get(tableName);
            if (!eventTypes.contains(ets)) {
                return false;
            }
        } else {
            return false;
        }

        return true;
    }

    /**
     * 读取canal的监控配置
     * Map<String,List<CanalEntry.EventType>>
     * map<"order_info",list("")>
     */
    public static Map<String,List<CanalEntry.EventType>> resolveMonitorProperties() {

        // 初始化配置参数 -> 表名的映射
        final HashMap<String, String> conf2TableName = new HashMap<>();

        // 初始化结果map
        final HashMap<String, List<CanalEntry.EventType>> monitorMap =
                new HashMap<>();

        //遍历entrySet
        for (Map.Entry<Object, Object> kv : canalProperties.entrySet()) {

            // 读取kv
            final String k = (String) kv.getKey();
            final String v = (String) kv.getValue();

            // TODO (1) 如果读取事件类型的配置
            if (k.endsWith(SUFFIX_CANAL_MONITOR_TABLE_TYPE)) {
                // 获取key
                final String tableConf = k.substring(0, k.length() - 6);
                // 从tableConf中获取tableName
                final String tableName = canalProperties.getProperty(tableConf);

                // if monitorMap中存在tableName,说明有list存在
                // 获取list
                // else
                // 创建新list
                // 解析events，添加
                List<CanalEntry.EventType> eventList = null;
                if (monitorMap.containsKey(tableName)) {
                    eventList = monitorMap.get(tableName);
                } else {
                    eventList = new LinkedList<>();
                }
                final String[] events = v.split(",");
                for (String event : events) {
                    eventList.add(CanalEntry.EventType.valueOf(event));
                }
                monitorMap.put(tableName, eventList);

            }

            // TODO (2) 如果读到对表的配置，添加到map中
            if (k.startsWith(PREFIX_CANAL_MONITOR_TABLE) && !k.endsWith(SUFFIX_CANAL_MONITOR_TABLE_TYPE)) {
                // 获取表名
                final String tableName = canalProperties.getProperty(k);
                // TODO 操作结果list
                final boolean isKeyContains = monitorMap.containsKey(tableName);
                if (!isKeyContains) {
                    // 说明没有添加过,创建一个新的list
                    // 把表名和listput进结果map中
                    final LinkedList<CanalEntry.EventType> eventList = new LinkedList<>();
                    monitorMap.put((String)v, eventList);
                }

            }

            // TODO (3) 如果匹配到kafka主题


        }

        return monitorMap;
    }


    // 保存表对应的事件类型和发送到kafka的主题
    @Data
    @AllArgsConstructor
    public static class EventTypesTopic {
        // 需要监控的事件类型
        private List<CanalEntry.EventType> eventTypes;
        // kafka的主题
        private String topic;
    }


    public static void main(String[] args) {
        final Map<String, List<CanalEntry.EventType>> monitorMap = resolveMonitorProperties();
        System.out.println(monitorMap.toString());
    }
}

