/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.agent.plugin.fetcher;

import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.common.pojo.agent.TaskResult;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_RETURN_PARAM_DATA;
import static org.apache.inlong.agent.plugin.fetcher.ManagerResultFormatter.getResultData;

public class TestManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestManager.class);
    private static final Gson GSON = new Gson();

    @Test
    public void testManager() {
        doTest("{\"success\":true,\"errMsg\":null,\"data\":{\"cmdConfigs\":[],\"dataConfigs\":[{\"ip\":\"9.135.95.77\",\"uuid\":null,\"inlongGroupId\":\"gt200000264848\",\"inlongStreamId\":\"st20000016794f\",\"op\":\"0\",\"taskId\":282,\"taskType\":3,\"taskName\":\"source_2648_475\",\"snapshot\":null,\"syncSend\":1,\"syncPartitionKey\":null,\"state\":1,\"predefinedFields\":null,\"timeZone\":\"GMT+8:00\",\"auditVersion\":null,\"extParams\":\"{\\\"predefinedFieldMap\\\":null,\\\"pattern\\\":\\\"/data/log/wedata_YYYYMMDDhh_[0-9]+/test.log_[0-9]+\\\",\\\"blackList\\\":null,\\\"timeOffset\\\":\\\"-1m\\\",\\\"properties\\\":{},\\\"lineEndPattern\\\":null,\\\"contentCollectType\\\":null,\\\"envList\\\":null,\\\"metaFields\\\":null,\\\"dataContentStyle\\\":\\\"CSV\\\",\\\"dataSeparator\\\":\\\",\\\",\\\"cycleUnit\\\":\\\"H\\\",\\\"maxFileCount\\\":\\\"30\\\",\\\"timeZone\\\":\\\"GMT+8:00\\\",\\\"retry\\\":false,\\\"startTime\\\":0,\\\"endTime\\\":0,\\\"auditVersion\\\":null}\",\"version\":1,\"deliveryTime\":null,\"dataReportType\":0,\"mqClusters\":null,\"topicInfo\":null,\"valid\":true},{\"ip\":\"9.135.95.77\",\"uuid\":null,\"inlongGroupId\":\"gt2000002657bc\",\"inlongStreamId\":\"st200000168815\",\"op\":\"0\",\"taskId\":291,\"taskType\":3,\"taskName\":\"source_2657_487\",\"snapshot\":null,\"syncSend\":1,\"syncPartitionKey\":null,\"state\":1,\"predefinedFields\":null,\"timeZone\":\"GMT+8:00\",\"auditVersion\":null,\"extParams\":\"{\\\"predefinedFieldMap\\\":null,\\\"pattern\\\":\\\"/data/log/wedata_YYYYMMDDhh_[0-9]+/test.log_[0-9]+\\\",\\\"blackList\\\":null,\\\"timeOffset\\\":\\\"0h\\\",\\\"properties\\\":{},\\\"lineEndPattern\\\":null,\\\"contentCollectType\\\":\\\"FULL\\\",\\\"envList\\\":null,\\\"metaFields\\\":null,\\\"dataContentStyle\\\":\\\"CSV\\\",\\\"dataSeparator\\\":\\\",\\\",\\\"cycleUnit\\\":\\\"H\\\",\\\"maxFileCount\\\":\\\"30\\\",\\\"timeZone\\\":\\\"GMT+8:00\\\",\\\"retry\\\":false,\\\"startTime\\\":0,\\\"endTime\\\":0,\\\"auditVersion\\\":null}\",\"version\":1,\"deliveryTime\":null,\"dataReportType\":0,\"mqClusters\":null,\"topicInfo\":null,\"valid\":true},{\"ip\":\"9.135.95.77\",\"uuid\":null,\"inlongGroupId\":\"devcloud_group_id\",\"inlongStreamId\":\"devcloud_stream_id\",\"op\":\"0\",\"taskId\":415,\"taskType\":3,\"taskName\":\"aa\",\"snapshot\":null,\"syncSend\":0,\"syncPartitionKey\":null,\"state\":1,\"predefinedFields\":null,\"timeZone\":\"GMT+8:00\",\"auditVersion\":null,\"extParams\":\"{\\\"predefinedFieldMap\\\":null,\\\"pattern\\\":\\\"/data/log/YYYYMMDDhh_[0-9]+/test.log_[0-9]+\\\",\\\"blackList\\\":null,\\\"timeOffset\\\":\\\"0h\\\",\\\"properties\\\":{},\\\"lineEndPattern\\\":null,\\\"contentCollectType\\\":null,\\\"envList\\\":null,\\\"metaFields\\\":null,\\\"dataContentStyle\\\":\\\"CSV\\\",\\\"dataSeparator\\\":\\\"|\\\",\\\"cycleUnit\\\":\\\"H\\\",\\\"maxFileCount\\\":\\\"100\\\",\\\"timeZone\\\":null,\\\"retry\\\":false,\\\"startTime\\\":0,\\\"endTime\\\":0,\\\"auditVersion\\\":null}\",\"version\":1,\"deliveryTime\":null,\"dataReportType\":0,\"mqClusters\":null,\"topicInfo\":null,\"valid\":true},{\"ip\":\"9.135.95.77\",\"uuid\":null,\"inlongGroupId\":\"devcloud_group_id\",\"inlongStreamId\":\"devcloud_stream_id\",\"op\":\"0\",\"taskId\":667,\"taskType\":3,\"taskName\":\"m\",\"snapshot\":null,\"syncSend\":0,\"syncPartitionKey\":null,\"state\":1,\"predefinedFields\":null,\"timeZone\":\"GMT+8:00\",\"auditVersion\":null,\"extParams\":\"{\\\"predefinedFieldMap\\\":{},\\\"pattern\\\":\\\"/data/log_minute/YYYYMMDD/hh/mm/test.log_[0-9]+\\\",\\\"blackList\\\":null,\\\"timeOffset\\\":\\\"0h\\\",\\\"properties\\\":{},\\\"lineEndPattern\\\":null,\\\"contentCollectType\\\":null,\\\"envList\\\":null,\\\"metaFields\\\":null,\\\"dataContentStyle\\\":\\\"CSV\\\",\\\"dataSeparator\\\":\\\"|\\\",\\\"cycleUnit\\\":\\\"m\\\",\\\"maxFileCount\\\":\\\"20\\\",\\\"timeZone\\\":null,\\\"retry\\\":false,\\\"startTime\\\":0,\\\"endTime\\\":0,\\\"auditVersion\\\":null}\",\"version\":1,\"deliveryTime\":null,\"dataReportType\":0,\"mqClusters\":null,\"topicInfo\":null,\"valid\":true}]}}",
                4);
    }

    private void doTest(String retFromManager, int taskNum) {
        TaskResult taskResult = null;
        JsonObject resultData = getResultData(retFromManager);
        JsonElement element = resultData.get(AGENT_MANAGER_RETURN_PARAM_DATA);
        taskResult = GSON.fromJson(element.getAsJsonObject(), TaskResult.class);
        List<TaskProfile> taskProfiles = new ArrayList<>();
        taskResult.getDataConfigs().forEach((config) -> {
            TaskProfile profile = TaskProfile.convertToTaskProfile(config);
            taskProfiles.add(profile);
        });
        Assert.assertEquals(taskProfiles.size(), taskNum);
    }
}
