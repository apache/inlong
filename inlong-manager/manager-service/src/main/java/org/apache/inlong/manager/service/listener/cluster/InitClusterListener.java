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

package org.apache.inlong.manager.service.listener.cluster;

import org.apache.inlong.manager.common.enums.ProcessEvent;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.pojo.workflow.form.process.ClusterResourceProcessForm;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.process.ProcessEventListener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * The listener for initial the Inlong cluster information.
 */
@Slf4j
@Service
public class InitClusterListener implements ProcessEventListener {

    @Override
    public ProcessEvent event() {
        return ProcessEvent.CREATE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        ClusterResourceProcessForm clusterProcessForm = (ClusterResourceProcessForm) context.getProcessForm();
        log.info("Begin to init cluster info for cluster tag={}", clusterProcessForm.getInlongClusterTag());
        return ListenerResult.success();
    }

}
