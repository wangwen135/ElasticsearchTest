package com.wwh.es.handleData;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * <pre>
 * 处理Telnet关键数据
 * </pre>
 * 
 * @author wwh
 * @date 2017年1月14日 下午6:49:36
 */
public class TelnetCritical {

    private static final String indexName = "hinge-content";

    private static final String nindexName = "critical";

    private static final String typeName = "Telnet";

    private static final String ntypeName = "d1";

    public static void main(String[] args) throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        BoolQueryBuilder query = QueryBuilders.boolQuery();

        query.must(new ExistsQueryBuilder("password"));

        // query.should(new ExistsQueryBuilder("request.Cookie"));

        SearchResponse scrollResp = client.prepareSearch(indexName).setTypes(typeName)

                .setScroll(new TimeValue(60000))

                .setQuery(query)
                // .setQuery(QueryBuilders.matchAllQuery())

                .setSize(500).execute().actionGet();

        JSONObject json, nJson = new JSONObject();

        long totalSize = 0;

        while (true) {

            BulkRequestBuilder bulkRequest = client.prepareBulk();

            // Date date = new Date();

            for (SearchHit hit : scrollResp.getHits().getHits()) {

                totalSize++;

                String id = hit.getId();

                String dss = hit.sourceAsString();

                // System.out.println(dss);

                json = JSON.parseObject(dss);

                // 每个一个都加上原来数据的ID便于查找
                nJson.put("sourceId", id);

                nJson.put("date", json.getDate("resolveTime"));

                nJson.put("taskId", getValue(json, "taskInfo", "taskId"));

                nJson.put("type", "Telnet");

                nJson.put("servers", json.get("dstAddr"));

                nJson.put("srcIP", json.get("srcAddr"));

                nJson.put("dstIP", json.get("dstAddr"));

                nJson.put("userName", json.get("userName"));

                nJson.put("password", json.get("password"));

                System.out.println(nJson.toJSONString());

                bulkRequest.add(new IndexRequest(nindexName, ntypeName).source(nJson));

            }

            bulkRequest.get();

            System.out.println("总数：" + totalSize);

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();

            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }

        }

        System.out.println("结束");

    }

    public static Object getValue(JSONObject jsonRow, String... path) {
        if (jsonRow == null || path == null) {
            return null;
        }

        if (path.length == 1) {

            return jsonRow.get(path[0]);

        } else {

            Object _obj = jsonRow.get(path[0]);
            String[] newCol = Arrays.copyOfRange(path, 1, path.length);

            if (_obj instanceof JSONObject) {
                return getValue((JSONObject) _obj, newCol);
            } else if (_obj instanceof JSONArray) {
                // 只取第一个
                JSONArray _array = (JSONArray) _obj;

                if (_array.size() < 1) {
                    return null;
                }

                return getValue(_array.getJSONObject(0), newCol);

            } else {
                return null;
            }

        }

    }

}
