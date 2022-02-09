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

package org.apache.inlong.manager.service.mocks;

import java.util.HashMap;
import java.util.Map;
import org.apache.inlong.manager.common.event.EventSelector;
import org.apache.inlong.manager.common.event.task.DataSourceOperateListener;
import org.apache.inlong.manager.common.event.task.SortOperateListener;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.ProcessForm;
import org.apache.inlong.manager.common.plugin.ProcessPlugin;
import org.apache.inlong.manager.service.workflow.business.UpdateBusinessWorkflowForm;
import org.apache.inlong.manager.service.workflow.business.UpdateBusinessWorkflowForm.OperateType;
import org.springframework.beans.factory.annotation.Autowired;

public class MockPlugin implements ProcessPlugin {

    public EventSelector stopProcessSelector = new EventSelector() {
        @Override
        public boolean accept(WorkflowContext context) {
            ProcessForm processForm = context.getProcessForm();
            if (processForm == null || !(processForm instanceof UpdateBusinessWorkflowForm)) {
                return false;
            }
            UpdateBusinessWorkflowForm form = (UpdateBusinessWorkflowForm) processForm;
            return form.getOperateType() == OperateType.SUSPEND;
        }
    };

    public EventSelector restartProcessSelector = new EventSelector() {
        @Override
        public boolean accept(WorkflowContext context) {
            ProcessForm processForm = context.getProcessForm();
            if (processForm == null || !(processForm instanceof UpdateBusinessWorkflowForm)) {
                return false;
            }
            UpdateBusinessWorkflowForm form = (UpdateBusinessWorkflowForm) processForm;
            return form.getOperateType() == OperateType.RESTART;
        }
    };

    public EventSelector deleteProcessSelector = new EventSelector() {
        @Override
        public boolean accept(WorkflowContext context) {
            ProcessForm processForm = context.getProcessForm();
            if (processForm == null || !(processForm instanceof UpdateBusinessWorkflowForm)) {
                return false;
            }
            UpdateBusinessWorkflowForm form = (UpdateBusinessWorkflowForm) processForm;
            return form.getOperateType() == OperateType.DELETE;
        }
    };

    @Autowired
    public Map<DataSourceOperateListener, EventSelector> createSourceOperateListeners() {
        Map<DataSourceOperateListener, EventSelector> listeners = new HashMap<>();
        listeners.put(new MockDeleteSourceListener(), deleteProcessSelector);
        listeners.put(new MockRestartSourceListener(), restartProcessSelector);
        listeners.put(new MockStopSourceListener(), stopProcessSelector);
        return listeners;
    }

    @Autowired
    public Map<SortOperateListener, EventSelector> createSortOperateListeners() {
        Map<SortOperateListener, EventSelector> listeners = new HashMap<>();
        listeners.put(new MockDeleteSortListener(), deleteProcessSelector);
        listeners.put(new MockRestartSortListener(), restartProcessSelector);
        listeners.put(new MockStopSortListener(), stopProcessSelector);
        return listeners;
    }

}
