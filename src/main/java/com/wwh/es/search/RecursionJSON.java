package com.wwh.es.search;

import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * <pre>
 * 解析JSON
 * 用于高亮
 * 非字符串类型的好像不能高亮
 * </pre>
 * 
 * @author wwh
 * @date 2017年2月21日 上午10:05:28
 */
public class RecursionJSON {

    private static final String JSON_STRING = "{\"currentCompany\":\"C3/CustomerContactChannels, Inc.\",\"educations\":[{\"activities\":\"\",\"degree\":\"MBA\",\"endDate\":\"– 1999\",\"fieldOfStudy\":\"Business\",\"notes\":\"\",\"schoolName\":\"Rollins College - Crummer Graduate School of Business\",\"startDate\":\"1997\"},{\"activities\":\"\",\"degree\":\"BachelorofScience(B.S.)\",\"endDate\":\"– 1995\",\"fieldOfStudy\":\"Management Information Systems\",\"notes\":\"\",\"schoolName\":\"University of Central Florida - College of Business Administration\",\"startDate\":\"1993\"}],\"fetchTime\":1484553016442,\"fullName\":\"Dennis Eaton\",\"headline\":\"Site Director\",\"industry\":\"Financial Services\",\"location\":\"Sandy, Utah\",\"pastCompanys\":[\"Sutter Physician Services\",\"Sigma Systems\",\"Convergys\"],\"positions\":[{\"company\":\"\",\"endDate\":\"\",\"startDate\":\"\",\"summary\":\"\",\"title\":\"Site Director\"},{\"company\":\"\",\"endDate\":\"November 2015\",\"startDate\":\"June 2015\",\"summary\":\"\",\"title\":\"Director\"},{\"company\":\"\",\"endDate\":\"May 2015\",\"startDate\":\"July 2014\",\"summary\":\"\",\"title\":\"Principal Consultant (Software Design and Configuration)\"},{\"company\":\"\",\"endDate\":\"July 2014\",\"startDate\":\"December 2010\",\"summary\":\"\",\"title\":\"Site Leader Call Center\"},{\"company\":\"\",\"endDate\":\"December 2010\",\"startDate\":\"November 2008\",\"summary\":\"\",\"title\":\"Site Leader Call Center\"},{\"company\":\"\",\"endDate\":\"November 2008\",\"startDate\":\"August 2000\",\"summary\":\"\",\"title\":\"Senior Client Services Manager\"},{\"company\":\"\",\"endDate\":\"August 2000\",\"startDate\":\"December 1998\",\"summary\":\"\",\"title\":\"Technical Services Manager\"},{\"company\":\"\",\"endDate\":\"December 1998\",\"startDate\":\"September 1996\",\"summary\":\"\",\"title\":\"Senior Financial Specialist\"},{\"company\":\"\",\"endDate\":\"January 1993\",\"startDate\":\"January 1990\",\"summary\":\"\",\"title\":\"Sales Manager\"},{\"company\":\"\",\"endDate\":\"January 1990\",\"startDate\":\"January 1988\",\"summary\":\"Walt Disney College Program\",\"title\":\"Retail, Transportation\"}],\"profileSkills\":[\"Vendor Management\",\"Call Centers\",\"Outsourcing\",\"Process Improvement\",\"Management\",\"Leadership\",\"BPO\",\"Program Management\",\"Workforce Management\",\"Team Leadership\"],\"saveTime\":1484553056759,\"schools\":[\"Rollins College - Crummer Graduate School of Business\"],\"sectionSkills\":[\"Customer Experience\",\"Team Management\",\"Service Delivery\",\"Team Building\",\"Call Center\",\"CRM\",\"Telecommunications\",\"Performance Management\",\"Customer Satisfaction\",\"Coaching\",\"Operations Management\",\"Business Analysis\",\"Business Process...\",\"Customer Service\",\"Cross-functional Team...\",\"Business Process...\",\"Change Management\",\"Project Management\",\"Account Management\"],\"state\":\"Utah\",\"fid\":\"5acbbef23a0e8559822b63f0f810ee51\"}";

    private static final String QUERY_STR = "September 1996";

    public static void main(String[] args) {

        JSONObject sourceJson = JSON.parseObject(JSON_STRING, JSONObject.class);

        // 高亮
        JSONArray highlightArray = new JSONArray();

        recursionHighlight(sourceJson, highlightArray, QUERY_STR, null);

        System.out.println("结果是：");
        System.out.println(highlightArray.toJSONString());
    }

    /**
     * 添加一个高亮键值对到结构中
     * 
     * @param highlightArray
     * @param key
     * @param value
     */
    public static void putHighlight(JSONArray highlightArray, String key, String value) {

        for (Object object : highlightArray) {
            JSONObject jsonObj = (JSONObject) object;
            if (jsonObj.isEmpty()) {
                // 这不科学
                continue;
            }

            for (Map.Entry<String, Object> entity : jsonObj.entrySet()) {
                if (key.equals(entity.getKey())) {// 匹配到之前的
                    JSONArray array = (JSONArray) entity.getValue();
                    array.add(value);
                    return;
                }
            }
        }

        // 添加一个新的
        JSONObject newObj = new JSONObject();
        JSONArray newArray = new JSONArray();
        newArray.add(value);
        newObj.put(key, newArray);
        highlightArray.add(newObj);

    }

    /**
     * 递归判断高亮
     * 
     * @param value
     * @param highlightArray
     * @param queryStr
     * @param key
     */
    public static void recursionHighlight(Object value, JSONArray highlightArray, String queryStr, String key) {

        if (value == null || highlightArray == null || queryStr == null) {
            return;
        }

        if (value instanceof JSONObject) {
            JSONObject jsonObj = (JSONObject) value;
            if (jsonObj.isEmpty()) {
                return;
            }

            for (Map.Entry<String, Object> entity : jsonObj.entrySet()) {

                String theKey;
                if (key == null || "".equals(key)) {
                    theKey = entity.getKey();
                } else {
                    theKey = key + "." + entity.getKey();
                }

                Object obj = entity.getValue();

                recursionHighlight(obj, highlightArray, queryStr, theKey);
            }

        } else if (value instanceof JSONArray) {
            JSONArray jsonArray = (JSONArray) value;
            if (jsonArray.isEmpty()) {
                return;
            }
            // 数组是没有key值的

            for (Object object : jsonArray) {
                recursionHighlight(object, highlightArray, queryStr, key);
            }

        } else {
            // 转换成 String 进行比较
            final String stringContent = value.toString();

            // 这里只进行匹配
            if (queryStr.equals(stringContent)) {
                // 如果匹配，则标记为高亮
                // TODO 这里的前后缀需要定义成常量
                String tagValue = "<em>" + stringContent + "</em>";
                putHighlight(highlightArray, key, tagValue);
            }

        }

    }
}
