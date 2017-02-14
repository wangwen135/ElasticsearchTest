package com.wwh.es.inout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class Input {

    /**
     * 文件保存路径
     */
    private static final String filePath = "F:\\数据文件\\p1\\p1_10.json";

    /**
     * 索引名称
     */
    private static final String indexName = "dwd-p1";

    /**
     * 类型名称
     */
    private static final String typeName = "dwdata";

    public static void main(String[] args) throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(new File(filePath)));

            String json;

            int count = 0;
            long total = 0;

            BulkRequestBuilder bulkRequest = client.prepareBulk();

            while ((json = br.readLine()) != null) {
                count++;
                total++;

                bulkRequest.add(client.prepareIndex(indexName, typeName).setSource(json));

                if (count == 500) {
                    BulkResponse bulkResponse = bulkRequest.get();
                    if (bulkResponse.hasFailures()) {
                        System.err.println("############ 出错了！！！！！");
                    }

                    bulkRequest = client.prepareBulk();
                    count = 0;
                    System.out.println("已经导入：" + total);
                }

            }

            if (count != 0) {
                BulkResponse bulkResponse = bulkRequest.get();
                if (bulkResponse.hasFailures()) {
                    System.err.println("############ 出错了！！！！！");
                }

                bulkRequest = client.prepareBulk();
                count = 0;
                System.out.println("已经导入：" + total);

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
