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
 * 处理Http的关键数据
 * </pre>
 * 
 * @author wwh
 * @date 2017年1月14日 下午6:49:36
 */
public class HttpCritical {

    private static final String indexName = "hinge-content";

    private static final String nindexName = "critical";

    private static final String typeName = "http";

    private static final String ntypeName = "d1";

    public static void main(String[] args) throws UnknownHostException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "hinge-es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.91"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9300));

        BoolQueryBuilder query = QueryBuilders.boolQuery();

        // query.should(new ExistsQueryBuilder("password"));

        query.must(new ExistsQueryBuilder("request.Cookie"));

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

                nJson.put("date", getValue(json, "response", "Date"));

                nJson.put("taskId", getValue(json, "taskInfo", "taskId"));

                nJson.put("type", "http");

                nJson.put("servers", getValue(json, "request", "Host"));

                nJson.put("srcIP", getValue(json, "request", "localAddress"));

                nJson.put("dstIP", getValue(json, "request", "remoteAddress"));

                String cookie = (String) getValue(json, "request", "Cookie");

                nJson.put("cookie", cookie);

                // 这里需要处理session
                nJson.put("sessionId", getSessionId(cookie));

                nJson.put("userName", json.get("Authorization"));

                nJson.put("password", json.get("Authorization"));

                // System.out.println(nJson.toJSONString());

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

    public static String getSessionId(String cookie) {
        String[] array = cookie.split(";");

        for (String entity : array) {
            String[] kv = entity.split("=", 2);
            String key = kv[0].trim();
            String value = kv[1].trim();

            if ("JSESSIONID".equalsIgnoreCase(key)) {
                return value;
            }

            if ("PHPSESSID".equalsIgnoreCase(key)) {
                return value;
            }

            if ("userid".equalsIgnoreCase(key)) {
                return value;
            }

            if ("SUID".equalsIgnoreCase(key)) {
                return value;
            }

        }

        for (String entity : array) {
            String[] kv = entity.split("=", 2);
            String key = kv[0].trim();
            String value = kv[1].trim();

            if (key.toUpperCase().contains("SESSION")) {
                return value;
            }

            if (key.endsWith("SID")) {
                return value;
            }

        }

        return null;
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
