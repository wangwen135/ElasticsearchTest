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
     * 一批获取数据
     */
    private static final int BATCH_SIZE = 10000;

    /**
     * 文件记录数
     */
    private static final int FILE_RECORD = 300000;

    /**
     * 文件保存路径
     */
    private static final String filePath = "F:\\数据文件\\bdmi\\";

    /**
     * 索引名称
     */
    private static final String indexName = "bdmi4";

    /**
     * 类型名称
     */
    private static final String typeName = "p1";

    public static void main(String[] args) throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        SearchResponse scrollResp = client.prepareSearch(indexName).setTypes(typeName).setQuery(QueryBuilders.matchAllQuery()).setSize(BATCH_SIZE).setScroll(new TimeValue(600000))
                .execute().actionGet();

        int fileIndex = 0;

        String outputFile = getFileName(fileIndex);

        BufferedWriter out = null;

        long totalSize = 0;

        try {

            out = new BufferedWriter(new FileWriter(outputFile));

            while (true) {
                for (SearchHit hit : scrollResp.getHits().getHits()) {

                    totalSize++;

                    out.write(hit.getId());// ID
                    out.write("\r\n");

                    out.write(hit.getSourceAsString());// 数据
                    out.write("\r\n");
                    out.flush();
                }

                System.out.println("已经导出：" + totalSize);

                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();

                if (scrollResp.getHits().getHits().length == 0) {
                    break;
                }

                if (totalSize > FILE_RECORD) {
                    out.flush();
                    out.close();
                    out = null;

                    System.out.println(outputFile);
                    System.out.println("导出结束，总共导出数据：" + totalSize);
                    totalSize = 0;
                    fileIndex++;
                    outputFile = getFileName(fileIndex);
                    out = new BufferedWriter(new FileWriter(outputFile));
                }
            }

            System.out.println(outputFile);
            System.out.println("导出结束，总共导出数据：" + totalSize);

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

    private static String getFileName(int fileIndex) {
        String outputFile = filePath + indexName + "#" + typeName + "_" + fileIndex + ".json";
        return outputFile;
    }

}
