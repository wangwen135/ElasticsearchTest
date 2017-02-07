package com.wwh.es.search;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;

public class aggsTest {

    public static void main(String[] args) throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        SearchRequestBuilder search = client.prepareSearch("bdmi").setTypes("p1");

        // search.setPostFilter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("state.raw","")));
        search.setQuery(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("state.raw", "")));

        search.addAggregation(AggregationBuilders.filter("xxf").filter(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("state.raw", ""))))
                .addAggregation(AggregationBuilders.terms("topResult").field("state.raw").size(10));

        // search.addAggregation(AggregationBuilders.terms("topResult").field("state.raw").size(10));

        SearchResponse sr = search.get();

        // 获取聚合结果
        Terms tos = sr.getAggregations().get("topResult");

        System.out.println("other\t\t" + tos.getSumOfOtherDocCounts());

        for (Bucket bucket : tos.getBuckets()) {

            System.out.print(bucket.getKeyAsString() + "\t\t");
            System.out.println(bucket.getDocCount());

        }

    }
}
