package cn.gp1996.gmall.utils;

import cn.gp1996.gmall.constants.GmallConstants;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author  gp1996
 * @date    2021-06-02
 * 使用的是searchbox 对es java api的封装
 */
public class PuESUtil {

    // 保存es配置
    private static Properties esProps = null;

    // 保存连接器工厂
    private static JestClientFactory esCliFactory = null;

    static {
        // 加载配置文件
        esProps = PropertiesUtil.getProperties(GmallConstants.GMALL_CONF_PATH);
    }

    private static void build() {
        esCliFactory = new JestClientFactory();
        // (使用建造者模式)创建httpClient配置
        final HttpClientConfig httpClientConfig =
                new HttpClientConfig.Builder(esProps.getProperty("es.connect.url"))
                .connTimeout(10 * 100)
                .readTimeout(10 * 100)
                .multiThreaded(true)
                .build();
        // System.out.println(esProps.getProperty("es.connect.url"));
        // 设置es客户端连接工厂
        esCliFactory.setHttpClientConfig(httpClientConfig);
    }

    /**
     * 获取es客户端
     * @return
     */
    public static JestClient getClient() {
        if (esCliFactory == null) {
            synchronized (PuESUtil.class) {
                if (esCliFactory == null) {
                    build();
                }
            }
        }

        return esCliFactory.getObject();
    }

    /**
     * 关闭客户端
     * @param cli
     */
    public static void closeClient(JestClient cli) {
        if (cli != null) {
            cli.shutdownClient();
        }
    }

    /**
     * 插入单条数据
     * @param cli
     * @param index
     * @param docId
     */
    public static void insert(JestClient cli, Object date, String index, String docId) {
        // 使用建造者创建一个新的index请求
        final Index put = new Index.Builder(date) //数据
                .index(index)
                .type("_doc")
                .id(docId)
                .build();

        // 使用客户端发送请求(同步)
        try {
            cli.execute(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量插入数据
     * @param cli
     * @param datas: 需要插入的数据
     * @param index
     * @param ids: 文档id的列表，和datas一一对应
     */
    public static void insertBulk(JestClient cli, List<String> datas,
                                  String index, List<String> ids) {
        // 初始化Bulk请求
        final Bulk.Builder bulkBuilder =
                new Bulk.Builder()
                .defaultIndex(index)
                .defaultType("_doc");

        // 循环构建index请求
        for (int i = 0; i < datas.size(); i++) {
            System.out.println(datas.get(i));
            final Index indexReq = new Index.Builder(datas.get(i))
                    .index(index)
                    .type("_doc")
                    .id(ids.get(i))
                    .build();
            bulkBuilder.addAction(indexReq);
        }

        // 使用客户端执行Bulk请求
        try {
            final Bulk bulk = bulkBuilder.build();
            cli.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    // 测试ES工具类
    public static void main(String[] args) throws IOException {
        // 1.获取客户端连接
        final JestClient jestClient = getClient();
        // 2.测试插入单条数据
        final String data1 = "{\n" +
                "  \"id\":\"1002\",\n" +
                "  \"movie_name\":\"姜子牙\"\n" +
                "}";
        final String index = "movie_test";
        final String docId = "1002";


        // 3.测试批量插入请求
        final String data2 = "{\n" +
                "  \"id\":\"1003\",\n" +
                "  \"movie_name\":\"姜子牙333\"\n" +
                "}";
        final String data3 = "{\n" +
                "  \"id\":\"1004\",\n" +
                "  \"movie_name\":\"姜子牙444\"\n" +
                "}";

        final ArrayList<String> datas = new ArrayList<>();
        datas.add(data2);
        datas.add(data3);

        final ArrayList<String> ids = new ArrayList<>();
        ids.add("1003");
        ids.add("1004");
    }

}
