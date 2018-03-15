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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * <pre>
 * 注意！！
 * 这个是有ID的
 * </pre>
 * 
 * @author wwh
 * @date 2017年2月16日 下午3:10:38
 */
public class InputSpecifiedId {

    /**
     * 文件保存路径
     */
    private static final String filePath = "F:\\tmp\\country.json";

    /**
     * 索引名称
     */
    private static final String indexName = "country_info";

    /**
     * 类型名称
     */
    private static final String typeName = "country";

    public static void main(String[] args) throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(new File(filePath)));

            String id;
            String json;
            
            JSONObject jsonObj;
            
            int count = 0;
            long total = 0;

            BulkRequestBuilder bulkRequest = client.prepareBulk();

            while ((json = br.readLine()) != null) {
                
                jsonObj =  JSON.parseObject(json);
                
                id = jsonObj.getString("SOV_A3");
                
                count++;
                total++;

                // 指定ID
                bulkRequest.add(client.prepareIndex(indexName, typeName, id).setSource(json));

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

            System.out.println("导入结束");

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
