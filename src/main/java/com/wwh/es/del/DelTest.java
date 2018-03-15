package com.wwh.es.del;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;

import com.alibaba.fastjson.JSON;

public class DelTest {

    public static void main(String[] args) throws Exception {
        // DelTest.batchDel();
        DelTest.query();
    }

    public static void query() throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "dap_es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9308))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9308));

        SearchRequestBuilder search = client.prepareSearch("downloads").setTypes("text");

        /**
         * <pre>
         * kpynyvym6xqi7wz2.onion 共删除数据：16871 
         * dtt6tdtgroj63iud.onion 共删除数据：103082
         * 3cvpkfx4gdnkcduj.onion 
         * bitmsgd3emeypwpj.onion 946524
         * </pre>
         */

        // search.setQuery(QueryBuilders.matchQuery("url", "txt"));

        search.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("url", "txt")).must(QueryBuilders.matchQuery("contentType", "plain")));

        // search.setQuery(QueryBuilders.termQuery("url", "kpynyvym6xqi7wz2"));

        // search.setQuery(QueryBuilders.termQuery("domain",
        // "bitmsgd3emeypwpj.onion"));

        SearchResponse sr = search.get();

        long total = sr.getHits().getTotalHits();

        System.out.println("总共命中：" + total);

        for (SearchHit searchHit : sr.getHits()) {
            System.out.println(searchHit.getId());
            System.out.println(searchHit.getSource().get("domain"));

            System.out.println(searchHit.getSource().get("contentType"));
        }

    }

    public static void batchDel() throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "dap_es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9308))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9308));

        SearchResponse scrollResp = client.prepareSearch("downloads").setTypes("text").setScroll(new TimeValue(60000))
                .setQuery(QueryBuilders.termQuery("domain", "3cvpkfx4gdnkcduj.onion")).setSize(500).execute().actionGet();

        long totalSize = 0;

        while (true) {

            BulkRequestBuilder bulkRequest = client.prepareBulk();

            for (SearchHit hit : scrollResp.getHits().getHits()) {
                totalSize++;

                String id = hit.getId();

                bulkRequest.add(client.prepareDelete("downloads", "text", id).request());

            }

            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                for (BulkItemResponse item : bulkResponse.getItems()) {
                    System.out.println(item.getFailureMessage());
                }
            } else {
                System.out.println("已经删除：" + totalSize);
            }

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();

            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }

        System.out.println("处理结束");

    }
}
