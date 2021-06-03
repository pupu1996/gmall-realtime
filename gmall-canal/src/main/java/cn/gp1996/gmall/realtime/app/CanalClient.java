package cn.gp1996.gmall.realtime.app;

import cn.gp1996.gmall.constants.GmallConstants;
import cn.gp1996.gmall.utils.KafkaUtil;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;

public class CanalClient {

    private static final String ORDER_TABLE = "order_info";

    public static void main(String[] args) {
        // 1.创建Canal连接器(工厂，单连接器)
        final CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop111", 11111),
                "example", "", ""
        );

        // 2.获取连接
        canalConnector.connect();

        // 3.指明要监控的数据库
        canalConnector.subscribe("gmall.*");

        try{
            while (true) {
                // 拉取数据Message
                // 100是一次拉取的entry数量
                final Message message = canalConnector.get(100);
                // 获取entry
                final List<CanalEntry.Entry> entries = message.getEntries();
                if (entries.size() <= 0) {
                    System.out.println("没有数据，休息一会儿");
                    Thread.sleep(5 * 1000L);
                } else {
                    // 如果有新增和更新的数据了，解析entry
                    for (CanalEntry.Entry entry : entries) {
                        // 获取表名
                        final String tblName = entry.getHeader().getTableName();
                        // 获取entry类型
                        final CanalEntry.EntryType entryType = entry.getEntryType();
                        // 判断entryType的类型是否为ROWDATA
                        if (CanalEntry.EntryType.ROWDATA.equals(entryType)) {
                            // 反序列化
                            final CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                            // 统计订单数据 -> 只需统计第一次下单行为 -> EventType = INSERT
                            // 获取EntryType
                            final CanalEntry.EventType eventType = rowChange.getEventType();
                            // 获取行数据列表
                            final List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                            handleChange(tblName, eventType, rowDatasList);
                        }
                    }
                }
            }
        } catch (InterruptedException | InvalidProtocolBufferException e) {
            e.printStackTrace();
        } finally {
            canalConnector.disconnect();
        }

    }

    /**
     * 处理变更数据
     * 需要是: (1) 表名是订单表 （2） 数据EventType为insert
     * @param tblName
     * @param et
     * @param rows
     */
    private static void handleChange(
            String tblName, CanalEntry.EventType et, List<CanalEntry.RowData> rows) {
        // 不满足条件，则退出
        if (!ORDER_TABLE.equals(tblName) ||
            !CanalEntry.EventType.INSERT.equals(et)
        ) return;

        // 初始化结果map arrayList -> jsonArray ; map -> jsonObject
        final HashMap<String, String> rowMap = new HashMap<String, String>();
        System.out.println("新增插入数据 " + rows.size() + " 条");
        // 遍历一条sql影响的行
        for (CanalEntry.RowData row : rows) {
            // 解析RowData数据结构
            for (CanalEntry.Column column : row.getAfterColumnsList()) {
                rowMap.put(column.getName(), column.getValue());
            }

            // 发送数据到Kafka
            KafkaUtil.send(GmallConstants.KAFKA_TOPIC_ORDER, JSONObject.toJSONString(rowMap));
            //System.out.println(JSONObject.toJSONString(rowMap));
            // 清空map
            rowMap.clear();
        }

    }
}

