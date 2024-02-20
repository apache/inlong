package org.apache.inlong.manager.service.sink;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import org.apache.inlong.manager.pojo.node.es.ElasticsearchAggregationsSumInfo;
import org.apache.inlong.manager.pojo.node.es.ElasticsearchAggregationsTermsInfo;
import org.apache.inlong.manager.pojo.node.es.ElasticsearchQueryBoolInfo;
import org.apache.inlong.manager.pojo.node.es.ElasticsearchQueryFieldInfo;
import org.apache.inlong.manager.pojo.node.es.ElasticsearchQueryInfo;
import org.apache.inlong.manager.pojo.node.es.ElasticsearchQuerySortInfo;
import org.apache.inlong.manager.pojo.node.es.ElasticsearchQuerySortValueInfo;
import org.apache.inlong.manager.pojo.node.es.ElasticsearchQueryTermInfo;
import org.apache.inlong.manager.pojo.node.es.ElasticsearchQueryTermValueInfo;
import org.apache.inlong.manager.pojo.node.es.ElasticsearchRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchJsonTest {

    private static final Gson GSON = new GsonBuilder().create();

    public static void main(String[] args) {

        Map<String,ElasticsearchQueryTermValueInfo> groupId = new HashMap<>();
        groupId.put("inlong_group_id",new ElasticsearchQueryTermValueInfo("groupId",1.0));
        ElasticsearchQueryTermInfo groupIdTerm = new ElasticsearchQueryTermInfo(groupId);

        Map<String,ElasticsearchQueryTermValueInfo> streamId = new HashMap<>();
        streamId.put("inlong_group_id",new ElasticsearchQueryTermValueInfo("streamId",1.0));
        ElasticsearchQueryTermInfo streamIdTerm = new ElasticsearchQueryTermInfo(streamId);

        List<ElasticsearchQueryTermInfo> termInfos = new ArrayList<>();
        termInfos.add(groupIdTerm);
        termInfos.add(streamIdTerm);

        ElasticsearchQueryBoolInfo  boolInfo = new ElasticsearchQueryBoolInfo(termInfos,1.0,true);
        ElasticsearchQueryInfo queryInfo = new ElasticsearchQueryInfo(boolInfo);

        Map<String,ElasticsearchQuerySortValueInfo> termValueInfoMap = new HashMap<>();
        ElasticsearchQuerySortValueInfo asc = new ElasticsearchQuerySortValueInfo("ASC");
        termValueInfoMap.put("log_ts",asc);
        List<Map<String,ElasticsearchQuerySortValueInfo>> list  = new ArrayList<>();
        list.add(termValueInfoMap);
        ElasticsearchQuerySortInfo sortInfo = new ElasticsearchQuerySortInfo(list);


        ElasticsearchQueryFieldInfo count = new ElasticsearchQueryFieldInfo("count");
        ElasticsearchAggregationsSumInfo countSum = new ElasticsearchAggregationsSumInfo(count);
        ElasticsearchQueryFieldInfo delay = new ElasticsearchQueryFieldInfo("delay");
        ElasticsearchAggregationsSumInfo delaySum = new ElasticsearchAggregationsSumInfo(delay);

        Map<String,ElasticsearchAggregationsSumInfo> aggregations = new HashMap<>();
        aggregations.put("count",countSum);
        aggregations.put("delay",delaySum);
        ElasticsearchAggregationsTermsInfo termsInfo = new ElasticsearchAggregationsTermsInfo("log_ts",
                Integer.MAX_VALUE, aggregations);
        Map<String,ElasticsearchAggregationsTermsInfo> terms = new HashMap<>();
        terms.put("terms", termsInfo);
        Map<String,Map<String,ElasticsearchAggregationsTermsInfo>> logts = new HashMap<>();
        logts.put("log_ts",terms);
        ElasticsearchRequest request = new ElasticsearchRequest();
        request.setFrom(1);
        request.setSize(10);
        request.setQuery(queryInfo);
        request.setSort(sortInfo);
        request.setAggregations(logts);

        System.out.println(GSON.toJson(request));
    }

}
