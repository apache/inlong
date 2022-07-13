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

package org.apache.inlong.manager.client.api.inner.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.pagehelper.PageInfo;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.service.StreamTransformApi;
import org.apache.inlong.manager.client.api.service.WorkflowApi;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.workflow.EventLogView;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;
import org.apache.inlong.manager.common.pojo.workflow.form.process.NewGroupProcessForm;
import org.apache.inlong.manager.common.util.JsonUtils;

import java.util.List;
import java.util.Map;

/**
 * Client for {@link WorkflowApi}.
 */
@Slf4j
public class WorkflowClient {

    private final WorkflowApi workflowApi;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public WorkflowClient(ClientConfiguration configuration) {
        workflowApi = ClientUtils.createRetrofit(configuration).create(WorkflowApi.class);
    }

    public WorkflowResult startInlongGroup(int taskId, NewGroupProcessForm newGroupProcessForm) {
        ObjectNode workflowTaskOperation = objectMapper.createObjectNode();
        workflowTaskOperation.putPOJO("transferTo", Lists.newArrayList());
        workflowTaskOperation.put("remark", "approved by system");

        ObjectNode inlongGroupApproveForm = objectMapper.createObjectNode();
        inlongGroupApproveForm.putPOJO("groupApproveInfo", newGroupProcessForm.getGroupInfo());
        inlongGroupApproveForm.putPOJO("streamApproveInfoList", newGroupProcessForm.getStreamInfoList());
        inlongGroupApproveForm.put("formName", "InlongGroupApproveForm");
        workflowTaskOperation.set("form", inlongGroupApproveForm);

        log.info("startInlongGroup workflowTaskOperation: {}", inlongGroupApproveForm);

        Map<String, Object> requestMap = JsonUtils.OBJECT_MAPPER.convertValue(workflowTaskOperation,
                new TypeReference<Map<String, Object>>() {
                });
        Response<WorkflowResult> response = ClientUtils.executeHttpCall(
                workflowApi.startInlongGroup(taskId, requestMap));
        ClientUtils.assertRespSuccess(response);

        return response.getData();
    }

    /**
     * get inlong group error messages
     */
    public List<EventLogView> getInlongGroupError(String inlongGroupId) {
        Response<PageInfo<EventLogView>> response = ClientUtils.executeHttpCall(
                workflowApi.getInlongGroupError(inlongGroupId, -1));
        ClientUtils.assertRespSuccess(response);
        return response.getData().getList();
    }
}
