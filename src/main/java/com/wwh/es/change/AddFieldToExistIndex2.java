package com.wwh.es.change;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 * <pre>
 * 添加国家属性
 * </pre>
 * 
 * @author wwh
 * @date 2017年2月9日 下午5:15:04
 */
public class AddFieldToExistIndex2 {

    private static final String indexName = "dnd-on";

    private static final String typeName = "dndata";

    public static void main(String[] args) throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        SearchResponse scrollResp = client.prepareSearch(indexName).setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(500).execute().actionGet();

        long totalSize = 0;

        while (true) {

            BulkRequestBuilder bulkRequest = client.prepareBulk();

            for (SearchHit hit : scrollResp.getHits().getHits()) {
                // 循环遍历
                totalSize++;
                String id = hit.getId();
                // 给每一个文档增加一个国家字段
                // 指定国家或者通过里面的内容来判断国家
                UpdateRequest ur = new UpdateRequest(indexName, typeName, id).doc("_country", "CH1");
                bulkRequest.add(ur);
            }
            bulkRequest.get();

            System.out.println("总数：" + totalSize);

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
            // Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
        System.out.println("结束");

    }
}
