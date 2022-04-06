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

package org.apache.inlong.manager.plugin.eventselect;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.pojo.workflow.form.ProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.UpdateGroupProcessForm;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.EventSelector;

@Slf4j
public class RestartProcessSelector implements EventSelector {
    @SneakyThrows
    @Override
    public boolean accept(WorkflowContext workflowContext) {
        String inlongGroupId = workflowContext.getProcessForm().getInlongGroupId();
        log.info("inlongGroupId:{} enter restartProcess listener", inlongGroupId);
        ProcessForm processForm = workflowContext.getProcessForm();
        if (processForm == null || !(processForm instanceof UpdateGroupProcessForm)) {
            log.info("inlongGroupId:{} not add restartProcess listener", inlongGroupId);
            return false;
        }
        UpdateGroupProcessForm updateGroupProcessForm = (UpdateGroupProcessForm) processForm;
        boolean flag = updateGroupProcessForm.getOperateType() == UpdateGroupProcessForm.OperateType.RESTART;
        if (!flag) {
            log.info("inlongGroupId:{} not add restartProcess listener, not RESTART", inlongGroupId);
            return false;
        }
        log.info("inlongGroupId:{} add restartProcess listener", inlongGroupId);
        return true;
    }
}
