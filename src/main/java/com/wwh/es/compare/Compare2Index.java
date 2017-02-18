package com.wwh.es.compare;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

public class Compare2Index {

    /**
     * 一批获取数据
     */
    private static final int BATCH_SIZE = 10000;

    /**
     * 文件记录数
     */
    private static final int FILE_RECORD = 3000000;

    /**
     * 文件保存路径
     */
    private static final String filePath = "F:\\数据文件\\compare\\";

    // 用源头的对比目标的，如果目标中不存在就写入到文件中

    /**
     * 用于对比的字段
     */
    private static final String compareField = "fetchTime";

    /**
     * 源索引
     */
    private static final String indexSrc = "dwd-p1";

    /**
     * 源索引类型
     */
    private static final String typeSrc = "dwdata";

    /**
     * 目标索引
     */
    private static final String indexTarget = "bdmi4";

    /**
     * 目标索引类型
     */
    private static final String typeTarget = "p1";

    public static void main3(String[] args) throws UnknownHostException {
        Settings settings = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        SearchRequestBuilder srb = client.prepareSearch(indexSrc).setTypes(typeSrc);

        SearchResponse response = srb.setQuery(QueryBuilders.matchQuery("fetchTime", 14841171017431L)).addField(compareField).setSize(1).get();

        for (SearchHit hit : response.getHits().getHits()) {

            System.out.println(hit);
            System.out.println(hit.getId());
            System.out.println(hit.getSourceAsString());
        }
    }

    public static void main(String[] args) throws UnknownHostException {

        Settings settings2 = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        // 没有测试过，先弄两个client对象吧
        TransportClient client2 = TransportClient.builder().settings(settings2).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        // 这个查询目标索引的用另外一个clinet对象
        SearchRequestBuilder srb = client2.prepareSearch(indexTarget).setTypes(typeTarget).addField(compareField).setSize(1);

        // #################################################################################

        Settings settings = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        SearchResponse scrollResp = client.prepareSearch(indexSrc).setTypes(typeSrc).setQuery(QueryBuilders.matchAllQuery()).setSize(BATCH_SIZE).setScroll(new TimeValue(600000))
                .execute().actionGet();

        int fileIndex = 0;

        String outputFile = getFileName(fileIndex);

        BufferedWriter out = null;

        BufferedWriter outNullValue = null;

        long totalSize = 0;

        try {

            out = new BufferedWriter(new FileWriter(outputFile));

            outNullValue = new BufferedWriter(new FileWriter(filePath + indexSrc + "_nullValue_.json"));

            SearchResponse _response;
            Object timeStamp;
            while (true) {
                for (SearchHit hit : scrollResp.getHits().getHits()) {

                    // 先获取源数据的时间戳
                    timeStamp = hit.getSource().get(compareField);

                    // 如果时间戳为空
                    if (timeStamp == null) {

                        outNullValue.write("nullT=");// 给ID增加一个固定的前缀
                        outNullValue.write(hit.getId());// ID
                        outNullValue.write("\r\n");

                        outNullValue.write(hit.getSourceAsString());// 数据
                        outNullValue.write("\r\n");
                        outNullValue.flush();

                        System.out.println("null timeStamp " + hit.getId());
                        continue;
                    }

                    _response = srb.setQuery(QueryBuilders.matchQuery(compareField, timeStamp)).get();

                    if (_response.getHits().getHits().length == 0) {
                        // 目标中没有数据
                        totalSize++;

                        out.write("lost=");// 给ID增加一个固定的前缀
                        out.write(hit.getId());// ID
                        out.write("\r\n");

                        out.write(hit.getSourceAsString());// 数据
                        out.write("\r\n");
                        out.flush();

                    }

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

            if (outNullValue != null) {
                try {
                    outNullValue.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println("查询结束");
    }

    private static String getFileName(int fileIndex) {
        String outputFile = filePath + indexTarget + "缺少的_" + fileIndex + ".json";
        return outputFile;
    }

}
