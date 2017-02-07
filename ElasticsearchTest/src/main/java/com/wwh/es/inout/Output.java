package com.wwh.es.inout;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

public class Output {
    /**
     * 文件保存路径
     */
    private static final String filePath = "D:\\temp\\esExport\\";

    /**
     * 索引名称
     */
    private static final String indexName = "critical";

    /**
     * 类型名称
     */
    private static final String typeName = "d1";

    public static void main(String[] args) throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        SearchResponse scrollResp = client.prepareSearch(indexName).setTypes(typeName).setQuery(QueryBuilders.matchAllQuery()).setSize(10000).setScroll(new TimeValue(600000))
                .execute().actionGet();

        String outputFile = filePath + indexName + "#" + typeName + ".json";

        BufferedWriter out = null;

        long totalSize = 0;

        try {

            out = new BufferedWriter(new FileWriter(outputFile));

            while (true) {
                for (SearchHit hit : scrollResp.getHits().getHits()) {

                    totalSize++;

                    out.write(hit.getSourceAsString());
                    out.write("\r\n");
                    out.flush();
                }

                System.out.println("已经导出：" + totalSize);

                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();

                if (scrollResp.getHits().getHits().length == 0) {
                    break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println("查询结束");
    }

}
