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

package org.apache.inlong.manager.service.thirdpart.mq;

import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeMqTopicRequest;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.workflow.newbusiness.CreateResourceWorkflowForm;
import org.apache.inlong.manager.workflow.core.event.ListenerResult;
import org.apache.inlong.manager.workflow.core.event.task.TaskEvent;
import org.apache.inlong.manager.workflow.core.event.task.TaskEventListener;
import org.apache.inlong.manager.workflow.exception.WorkflowListenerException;
import org.apache.inlong.manager.workflow.model.WorkflowContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Create a listener for MQ resource tasks
 */
@Component
@Slf4j
public class CreateTubeTopicTaskListener implements TaskEventListener {

    @Autowired
    private TubeMqOptService tubeMqOptService;
    @Autowired
    private BusinessService businessService;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        CreateResourceWorkflowForm form = (CreateResourceWorkflowForm) context.getProcessForm();

        log.info("begin create tube topic for groupId={}", form.getInlongGroupId());
        String groupId = form.getInlongGroupId();

        try {
            BusinessInfo businessInfo = businessService.get(groupId);
            String topicName = businessInfo.getMqResourceObj();
            AddTubeMqTopicRequest request = new AddTubeMqTopicRequest();
            request.setUser("inlong-manager");
            AddTubeMqTopicRequest.AddTopicTasksBean tasksBean = new AddTubeMqTopicRequest.AddTopicTasksBean();
            tasksBean.setTopicName(topicName);
            request.setAddTopicTasks(Collections.singletonList(tasksBean));
            tubeMqOptService.createNewTopic(request);

            log.info("finish to create tube topic for groupId={}", groupId);
        } catch (Exception e) {
            log.error("create tube topic for groupId={} error, exception {} ", groupId, e.getMessage(), e);
        }
        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }
}
