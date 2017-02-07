package com.wwh.es.search;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.lucene.queryparser.simple.SimpleQueryParser;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.search.SearchHit;

public class RetrievalTest {
    public static void main(String[] args) throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        SearchRequestBuilder search = client.prepareSearch("hinge-content");
                //.setTypes("smtp","pop3","imap");
        
        //SimpleQueryStringBuilder qs = new SimpleQueryStringBuilder("+ftp -ftpuser");
        
        QueryStringQueryBuilder qs = new QueryStringQueryBuilder("ftp");
        
        
        // 最匹配的在前
        qs.useDisMax(true);
        search.setQuery(qs);

        SearchResponse response = search.execute().actionGet();

        System.out.println("总记录数：" + response.getHits().getTotalHits());

        for (SearchHit hit : response.getHits()) {

            String dss = hit.sourceAsString();

            System.out.println(hit.getType());
            System.out.println(dss);
            System.out.println();

        }

    }
}
