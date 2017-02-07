package com.wwh.es.change;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortParseElement;

/**
 * <pre>
 *  添加或者修改字段
 *  经过测试的
 * </pre>
 * 
 * @author wwh
 * @date 2017年1月3日 下午5:11:53
 */
public class AddFieldToExistIndex {

    private static final String indexName = "dnd-bn";

    private static final String typeName = "dndata";

    public static void main(String[] args) throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        SearchResponse scrollResp = client.prepareSearch(indexName).addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                // .addSort("_id", SortOrder.ASC)
                .setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(500).execute().actionGet();

        long totalSize = 0;

        while (true) {

            BulkRequestBuilder bulkRequest = client.prepareBulk();
            Date date = new Date();

            for (SearchHit hit : scrollResp.getHits().getHits()) {
                // 循环遍历
                totalSize++;
                String id = hit.getId();
                // 给每一个文档增加一个字段
                UpdateRequest ur = new UpdateRequest(indexName, typeName, id).doc("_input_datetime_", date);
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
