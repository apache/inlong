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

package org.apache.inlong.manager.workflow.core.processor;

import org.apache.inlong.manager.common.workflow.WorkflowDataAccessor;
import org.apache.inlong.manager.common.event.process.ProcessEvent;
import org.apache.inlong.manager.common.event.process.ProcessEventNotifier;
import org.apache.inlong.manager.workflow.core.impl.WorkflowEventNotifier;
import org.apache.inlong.manager.common.model.Action;
import org.apache.inlong.manager.common.model.ProcessState;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.Process;
import org.apache.inlong.manager.common.model.definition.ProcessForm;
import org.apache.inlong.manager.common.model.definition.StartEvent;
import org.apache.inlong.manager.common.model.instance.ProcessInstance;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;

import java.util.Date;

/**
 * Start event handler
 */
public class StartEventProcessor extends AbstractNextableElementProcessor<StartEvent> {

    private WorkflowDataAccessor workflowDataAccessor;

    private ProcessEventNotifier processEventNotifier;

    public StartEventProcessor(WorkflowDataAccessor workflowDataAccessor, WorkflowEventNotifier workflowEventNotifier) {
        this.workflowDataAccessor = workflowDataAccessor;
        this.processEventNotifier = workflowEventNotifier.getProcessEventNotifier();
    }

    @Override
    public Class<StartEvent> watch() {
        return StartEvent.class;
    }

    @Override
    public void create(StartEvent startEvent, WorkflowContext context) {
        String applicant = context.getApplicant();
        Process process = context.getProcess();
        ProcessForm form = context.getProcessForm();
        if (process.getFormClass() != null) {
            Preconditions.checkNotNull(form, "form cannot be null");
            Preconditions.checkTrue(form.getClass().isAssignableFrom(process.getFormClass()),
                    () -> "form type not match, should be class " + process.getFormClass());
            form.validate();
        } else {
            Preconditions.checkNull(form, "no form required");
        }
        ProcessInstance processInstance = createProcessInstance(applicant, process, form);
        context.setProcessInstance(processInstance);
        context.setActionContext(new WorkflowContext.ActionContext().setAction(Action.START));
    }

    @Override
    public boolean pendingForAction(WorkflowContext context) {
        return false;
    }

    @Override
    public boolean complete(WorkflowContext context) {
        processEventNotifier.notify(ProcessEvent.CREATE, context);
        return true;
    }

    private ProcessInstance createProcessInstance(String applicant, Process process, ProcessForm form) {
        ProcessInstance processInstance = new ProcessInstance()
                .setName(process.getName())
                .setDisplayName(process.getDisplayName())
                .setType(process.getType())
                .setTitle(form.getTitle())
                .setInlongGroupId(form.getInlongGroupId())
                .setApplicant(applicant)
                .setState(ProcessState.PROCESSING.name())
                .setFormData(JsonUtils.toJson(form))
                .setStartTime(new Date())
                .setHidden(process.getHidden());
        this.workflowDataAccessor.processInstanceStorage().insert(processInstance);
        Preconditions.checkNotNull(processInstance.getId(), "process inst id cannot be null");
        return processInstance;
    }
}
