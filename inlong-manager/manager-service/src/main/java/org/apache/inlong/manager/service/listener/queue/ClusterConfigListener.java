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

import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.pojo.workflow.form.process.ClusterResourceProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.service.cluster.ClusterConfigService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.ClusterOperateListener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Create cluster config listener for cluster tag
 */
@Slf4j
@Component
public class ClusterConfigListener implements ClusterOperateListener {

    @Autowired
    private ClusterConfigService clusterConfigService;

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
        String clusterTag = clusterProcessForm.getInlongClusterTag();
        String operator = context.getOperator();
        clusterConfigService.refresh(clusterTag, operator);
        log.info("success to execute ClusterConfigListener for clusterTag={}",
                clusterProcessForm.getInlongClusterTag());
        return ListenerResult.success("success");
    }

}
