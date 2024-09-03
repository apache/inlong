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

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.plugin.util.FlinkUtils;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.StreamResourceProcessForm;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;

import lombok.extern.slf4j.Slf4j;

/**
 * Listener for startup the Sort task for InlongStream
 */
@Slf4j
public class StartupStreamListener implements SortOperateListener {

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    /**
     * Currently, the process of starting Sort tasks has been initiated in {@link StartupSortListener}.
     * <p/>Because the Sort task is only associated with InlongGroup, no need to start it for InlongStream.
     */
    @Override
    public boolean accept(WorkflowContext workflowContext) {
        ProcessForm processForm = workflowContext.getProcessForm();
        String groupId = processForm.getInlongGroupId();
        if (!(processForm instanceof StreamResourceProcessForm)) {
            log.info("not add startup stream listener, not StreamResourceProcessForm for groupId [{}]", groupId);
            return false;
        }

        StreamResourceProcessForm streamProcessForm = (StreamResourceProcessForm) processForm;
        String streamId = streamProcessForm.getStreamInfo().getInlongStreamId();
        if (streamProcessForm.getGroupOperateType() != GroupOperateType.INIT) {
            log.info("not add startup stream listener, as the operate was not INIT for groupId [{}] streamId [{}]",
                    groupId, streamId);
            return false;
        }

        log.info("add startup stream listener for groupId [{}] streamId [{}]", groupId, streamId);
        return InlongConstants.STANDARD_MODE.equals(streamProcessForm.getGroupInfo().getInlongGroupMode());
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        ProcessForm processForm = context.getProcessForm();
        StreamResourceProcessForm streamResourceProcessForm = (StreamResourceProcessForm) processForm;
        InlongStreamInfo streamInfo = streamResourceProcessForm.getStreamInfo();
        log.info("inlong stream :{} ext info: {}", streamInfo.getInlongStreamId(), streamInfo.getExtList());

        String jobName = FlinkUtils.genFlinkJobName(processForm, streamInfo);
        return FlinkUtils.submitFlinkJob(streamInfo, jobName);
    }

}
