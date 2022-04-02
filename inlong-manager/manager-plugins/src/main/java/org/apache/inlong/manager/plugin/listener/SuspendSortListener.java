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
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.plugin.flink.FlinkService;
import org.apache.inlong.manager.plugin.flink.ManagerFlinkTask;
import org.apache.inlong.manager.plugin.flink.dto.FlinkInfo;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.plugin.util.FlinkUtils.getExceptionStackMsg;

@Slf4j
public class SuspendSortListener implements SortOperateListener {
    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        String inlongGroupId = context.getProcessForm().getInlongGroupId();
        ObjectMapper objectMapper = new ObjectMapper();
        UpdateGroupProcessForm updateGroupProcessForm = (UpdateGroupProcessForm) context.getProcessForm();
        InlongGroupInfo inlongGroupInfo = updateGroupProcessForm.getGroupInfo();
        List<InlongGroupExtInfo> inlongGroupExtInfos = inlongGroupInfo.getExtList();
        log.info("inlongGroupExtInfos:{}", inlongGroupExtInfos);
        Map<String, String> kvConf = inlongGroupExtInfos.stream().collect(Collectors.toMap(
                InlongGroupExtInfo::getKeyName, InlongGroupExtInfo::getKeyValue));
        String sortExt = kvConf.get(InlongGroupSettings.SORT_PROPERTIES);
        if (StringUtils.isEmpty(sortExt)) {
            String message = String.format("inlongGroupId:%s not add suspendProcess listener,sortProperties is empty",
                    inlongGroupId);
            log.warn(message);
            return ListenerResult.fail(message);
        }
        Map<String, String> result = objectMapper.convertValue(objectMapper.readTree(sortExt),
                new TypeReference<Map<String, String>>(){});
        kvConf.putAll(result);
        if (StringUtils.isEmpty(kvConf.get(InlongGroupSettings.SORT_JOB_ID))) {
            String message = String.format("inlongGroupId:%s not add suspendProcess listener,SORT_JOB_ID is empty",
                    inlongGroupId);
            log.warn(message);
            return ListenerResult.fail(message);
        }
        FlinkInfo flinkInfo = new FlinkInfo();
        flinkInfo.setJobId(kvConf.get(InlongGroupSettings.SORT_JOB_ID));
        FlinkService flinkService = new FlinkService();
        ManagerFlinkTask managerFlinkTask = new ManagerFlinkTask(flinkService);

        try {
            managerFlinkTask.stop(flinkInfo);
        } catch (Exception e) {
            log.error("pause exception ", e);
            flinkInfo.setException(true);
            flinkInfo.setExceptionMsg(getExceptionStackMsg(e));
            managerFlinkTask.pollFlinkStatus(flinkInfo);
        }
        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }
}
