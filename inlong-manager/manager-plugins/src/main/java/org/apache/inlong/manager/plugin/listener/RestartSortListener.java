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

package org.apache.inlong.manager.plugin.listener;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.plugin.dto.FlinkConf;
import org.apache.inlong.manager.plugin.dto.LoginConf;
import org.apache.inlong.manager.plugin.flink.Constants;
import org.apache.inlong.manager.plugin.flink.FlinkService;
import org.apache.inlong.manager.plugin.flink.ManagerFlinkTask;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.plugin.flink.FlinkUtils.translateFromEndpont;

@Slf4j
public class RestartSortListener implements SortOperateListener {

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        String inlongGroupId = context.getProcessForm().getInlongGroupId();
        ObjectMapper objectMapper = new ObjectMapper();
        UpdateGroupProcessForm updateGroupProcessForm = (UpdateGroupProcessForm) context.getProcessForm();
        String jobName = Constants.INLONG + context.getProcessForm().getInlongGroupId();
        InlongGroupInfo inlongGroupInfo = updateGroupProcessForm.getGroupInfo();
        List<InlongGroupExtInfo> inlongGroupExtInfos = inlongGroupInfo.getExtList();
        log.info("inlongGroupExtInfos:{}", inlongGroupExtInfos);
        Map<String, String> kvConf = inlongGroupExtInfos.stream()
                .collect(Collectors.toMap(InlongGroupExtInfo::getKeyName, InlongGroupExtInfo::getKeyValue));
        String sortExt = kvConf.get(InlongGroupSettings.SORT_PROPERTIES);
        if (StringUtils.isEmpty(sortExt)) {
            String message = String.format("inlongGroupId:%s not add restartProcess listener,sortProperties is empty",
                    inlongGroupId);
            log.warn(message);
            return ListenerResult.fail(message);
        }
        Map<String, String> result = objectMapper.convertValue(objectMapper.readTree(sortExt),
                new TypeReference<Map<String, String>>(){});
        kvConf.putAll(result);
        if (StringUtils.isEmpty(kvConf.get(InlongGroupSettings.SORT_JOB_ID))) {
            String message = String.format("inlongGroupId:%s not add restartProcess listener,SORT_JOB_ID is empty",
                    inlongGroupId);
            log.warn(message);
            return ListenerResult.fail(message);
        }
        String dataFlows = kvConf.get(InlongGroupSettings.DATA_FLOW);
        if (StringUtils.isEmpty(dataFlows)) {
            String message = String.format("inlongGroupId:{} not add restartProcess listener,dataflows is empty",
                    inlongGroupId);
            log.warn(message);
            return ListenerResult.fail(message);
        }
        Map<String, JsonNode> dataflowMap = objectMapper.convertValue(objectMapper.readTree(dataFlows),
                new TypeReference<Map<String, JsonNode>>(){});
        Optional<JsonNode> dataflowOptional = dataflowMap.values().stream().findFirst();
        JsonNode dataFlow = null;
        if (dataflowOptional.isPresent()) {
            dataFlow = dataflowOptional.get();
        }
        if (Objects.isNull(dataFlow)) {
            String message = String.format("inlongGroupId:{} not add restartProcess listener,dataflow is empty",
                    inlongGroupId);
            log.warn(message);
            return ListenerResult.fail(message);
        }
        FlinkConf flinkConf = new FlinkConf();

        flinkConf.setRegion(kvConf.get(InlongGroupSettings.REGION));
        flinkConf.setClusterId(kvConf.get(InlongGroupSettings.CLUSTER_ID));
        flinkConf.setJobId(kvConf.get(InlongGroupSettings.SORT_JOB_ID));
        flinkConf.setResourceIds(kvConf.get(Constants.RESOURCE_ID));
        LoginConf loginConf = translateFromEndpont(kvConf.get(InlongGroupSettings.SORT_URL));
        flinkConf.setAddress(loginConf.getRestAddress());
        flinkConf.setPort(loginConf.getRestPort());
        FlinkService flinkService = new FlinkService(flinkConf.getAddress(),flinkConf.getPort());
        ManagerFlinkTask managerFlinkTask = new ManagerFlinkTask(flinkService);
        managerFlinkTask.genPath(flinkConf,dataFlow.toString());
        managerFlinkTask.restart(flinkConf);

        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }
}
