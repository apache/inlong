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

package org.apache.inlong.manager.service.listener.queue;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.pojo.workflow.form.process.ClusterResourceProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.ClusterResourceProcessForm.GroupFullInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.service.resource.queue.QueueResourceOperatorFactory;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.ClusterOperateListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Create cluster config listener for cluster tag
 */
@Slf4j
@Component
public class ClusterConfigListener implements ClusterOperateListener {

    @Autowired
    private QueueResourceOperatorFactory queueOperatorFactory;
    @Autowired
    private InlongClusterEntityMapper clusterEntityMapper;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public boolean accept(WorkflowContext context) {
        ProcessForm processForm = context.getProcessForm();
        return processForm instanceof ClusterResourceProcessForm;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        ClusterResourceProcessForm clusterProcessForm = (ClusterResourceProcessForm) context.getProcessForm();
        log.info("begin to execute ClusterConfigListener for clusterTag={}", clusterProcessForm.getInlongClusterTag());
        List<GroupFullInfo> groupFullInfoList = clusterProcessForm.getGroupFullInfoList();
        // String operator = context.getOperator();
        // for (GroupFullInfo groupFullInfo : groupFullInfoList) {
        // InlongGroupInfo groupInfo = groupFullInfo.getGroupInfo();
        // QueueResourceOperator queueOperator = queueOperatorFactory.getInstance(groupInfo.getMqType());
        // queueOperator.createQueueForGroup(groupInfo, operator);
        // for (InlongStreamInfo streamInfo : groupFullInfo.getStreamInfoList()) {
        // queueOperator.createQueueForStream(groupInfo, streamInfo, operator);
        // }
        // }

        log.info("success to execute ClusterConfigListener for clusterTag={}", clusterProcessForm.getInlongClusterTag());
        return ListenerResult.success("success");
    }

}
