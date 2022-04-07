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
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.plugin.flink.Constants;
import org.apache.inlong.manager.plugin.flink.FlinkService;
import org.apache.inlong.manager.plugin.flink.ManagerFlinkTask;
import org.apache.inlong.manager.plugin.flink.dto.FlinkInfo;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.plugin.util.FlinkUtils.getExceptionStackMsg;

@Slf4j
public class StartupSortListener implements SortOperateListener {
    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        GroupResourceProcessForm groupResourceProcessForm = (GroupResourceProcessForm) context.getProcessForm();
        InlongGroupInfo inlongGroupInfo = groupResourceProcessForm.getGroupInfo();
        List<InlongGroupExtInfo> inlongGroupExtInfos = inlongGroupInfo.getExtList();
        log.info("inlongGroupExtInfos:{}", inlongGroupExtInfos);
        Map<String, String> kvConf = inlongGroupExtInfos.stream().filter(v -> StringUtils.isNotEmpty(v.getKeyName())
                && StringUtils.isNotEmpty(v.getKeyValue())).collect(Collectors.toMap(
                InlongGroupExtInfo::getKeyName,
                InlongGroupExtInfo::getKeyValue));
        String dataFlows = kvConf.get(InlongGroupSettings.DATA_FLOW);
        String inlongGroupId = context.getProcessForm().getInlongGroupId();
        if (StringUtils.isEmpty(dataFlows)) {
            String message = String.format("inlongGroupId:%s not exec startupProcess listener, dataflows is empty",
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
            String message = String.format("groupId [%s] not add startupProcess listener, "
                    + "as the sortProperties is empty", inlongGroupId);
            log.warn(message);
            ListenerResult.fail(message);
        }

        String sortExt = kvConf.get(InlongGroupSettings.SORT_PROPERTIES);
        if (StringUtils.isNotEmpty(sortExt)) {
            Map<String, String> result = objectMapper.convertValue(objectMapper.readTree(sortExt),
                    new TypeReference<Map<String, String>>(){});
            kvConf.putAll(result);
        }
        FlinkInfo flinkInfo = new FlinkInfo();
        parseDataflow(dataFlow, flinkInfo);

        String sortUrl = kvConf.get(InlongGroupSettings.SORT_URL);
        flinkInfo.setEndpoint(sortUrl);

        String jobName = Constants.INLONG + context.getProcessForm().getInlongGroupId();
        flinkInfo.setJobName(jobName);

        flinkInfo.setInlongStreamInfoList(groupResourceProcessForm.getInlongStreamInfoList());

        FlinkService flinkService = new FlinkService(flinkInfo.getEndpoint());
        ManagerFlinkTask managerFlinkTask = new ManagerFlinkTask(flinkService);
        managerFlinkTask.genPath(flinkInfo, dataFlow.toString());

        try {
             managerFlinkTask.start(flinkInfo);
            log.info("the jobId {} submit success", flinkInfo.getJobId());
        } catch (Exception e) {
            log.warn("startup exception: ", e);
            managerFlinkTask.pollFlinkStatus(flinkInfo);
            flinkInfo.setException(true);
            flinkInfo.setExceptionMsg(getExceptionStackMsg(e));
            managerFlinkTask.pollFlinkStatus(flinkInfo);
        }

        managerFlinkTask.pollFlinkStatus(flinkInfo);

        saveInfo(context.getProcessForm().getInlongGroupId(), InlongGroupSettings.SORT_JOB_ID, flinkInfo.getJobId(),
                inlongGroupExtInfos);

        managerFlinkTask.pollFlinkStatus(flinkInfo);
        return ListenerResult.success();
    }

    /**
     * save info
     * @param inlongGroupId
     * @param keyName
     * @param keyValue
     * @param inlongGroupExtInfos
     */
    private void saveInfo(String inlongGroupId, String keyName, String keyValue,
            List<InlongGroupExtInfo> inlongGroupExtInfos) {
        InlongGroupExtInfo inlongGroupExtInfo = new InlongGroupExtInfo();
        inlongGroupExtInfo.setInlongGroupId(inlongGroupId);
        inlongGroupExtInfo.setKeyName(keyName);
        inlongGroupExtInfo.setKeyValue(keyValue);
        inlongGroupExtInfos.add(inlongGroupExtInfo);
    }

    /**
     * init FlinkConf
     * @param dataflow
     * @param flinkInfo
     */
    private void parseDataflow(JsonNode dataflow, FlinkInfo flinkInfo)  {
        JsonNode sourceInfo = dataflow.get(Constants.SOURCE_INFO);
        String sourceType = sourceInfo.get(Constants.TYPE).asText();
        flinkInfo.setSourceType(sourceType);
        JsonNode sinkInfo = dataflow.get(Constants.SINK_INFO);
        String sinkType = sinkInfo.get(Constants.TYPE).asText();
        flinkInfo.setSinkType(sinkType);
    }

    @Override
    public boolean async() {
        return false;
    }
}
