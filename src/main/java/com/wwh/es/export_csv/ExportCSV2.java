package com.wwh.es.export_csv;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * <pre>
 * 导出ES中的数据，转成CSV格式
 * 
 * 如果是数组结构的，只会导出数组中的第一条
 * 
 * </pre>
 * 
 * @author wwh
 * @date 2017年1月14日 下午5:33:23
 */
public class ExportCSV2 {

    /**
     * 文件保存路径
     */
    private static final String filePath = "D:\\temp\\esExport2\\";

    /**
     * 索引名称
     */
    private static final String indexName = "site";

    /**
     * 类型名称
     */
    private static final String typeName = "text";

    /**
     * 递归解析字段名称
     * 
     * @param colList
     * @param parent
     * @param json
     */
    public static void traversalCol(List<String> colList, String parent, JSONObject json) {
        if (json == null) {
            return;
        }

        if (json.size() == 1) {

            if (json.containsKey("type")) {
                colList.add(parent);
                return;
            }

            if (json.containsKey("properties")) {

                JSONObject _json = json.getJSONObject("properties");

                traversalCol(colList, parent, _json);

                return;
            }

        }

        for (Map.Entry<String, Object> en : json.entrySet()) {

            String key;
            if (parent == null || "".equals(parent)) {
                key = en.getKey();
            } else {
                key = parent + "." + en.getKey();
            }

            Object _obj = en.getValue();

            if (_obj instanceof String) {

                return;

            } else if (_obj instanceof JSONObject) {

                JSONObject obj2 = (JSONObject) _obj;
                if(obj2.containsKey("type")) {
                    colList.add(key);
                    continue;
                }
                
                traversalCol(colList, key, (JSONObject) _obj);

            }
        }
    }

    public static void main(String[] args) throws IOException {

        Settings settings = Settings.settingsBuilder().put("cluster.name", "dap_es").build();
        TransportClient client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.93"), 9308))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.1.92"), 9308));

        // 需要先获取映射，解析CSV文件的标题
        GetIndexResponse indexR = client.admin().indices().prepareGetIndex().setIndices(indexName).get();

        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> indexMap = indexR.getMappings();

        ImmutableOpenMap<String, MappingMetaData> indexMetaData = indexMap.get(indexName);

        MappingMetaData mmd = indexMetaData.get(typeName);

        List<String> columnList = new ArrayList<>();
        columnList.add("_id");

        JSONObject json = JSON.parseObject(mmd.source().toString());

        // 这是映射开始的位置
        json = json.getJSONObject(typeName).getJSONObject("properties");

        traversalCol(columnList, null, json);

        System.out.println(columnList);

        String[] columns = columnList.toArray(new String[] {});

        SearchResponse scrollResp = client.prepareSearch(indexName).setTypes(typeName)
                // .addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC)
                // .addSort("_id", SortOrder.ASC)
                .setScroll(new TimeValue(60000)).setQuery(QueryBuilders.matchAllQuery()).setSize(500).execute().actionGet();

        String outputFile = filePath + indexName + "#" + typeName + ".csv";

        FileWriter fileWriter = new FileWriter(outputFile);

        CSVPrinter csvPrinter = CSVFormat.DEFAULT.withHeader(columns).print(fileWriter);

        long totalSize = 0;

        JSONObject jsonRow;

        while (true) {

            for (SearchHit hit : scrollResp.getHits().getHits()) {
                totalSize++;

                String id = hit.getId();

                csvPrinter.print(id);

                jsonRow = JSON.parseObject(hit.sourceAsString());

                for (int i = 1; i < columns.length; i++) {
                    csvPrinter.print(getValue(jsonRow, columns[i].split("\\.")));
                }

                csvPrinter.println();

            }

            System.out.println("已经处理：" + totalSize);

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();

            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }

        System.out.println("处理结束");

        fileWriter.flush();

        fileWriter.close();

    }

    /**
     * 根据路径获取值
     * 
     * @param jsonRow
     * @param path
     * @return
     */
    public static Object getValue(JSONObject jsonRow, String[] path) {
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
