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

package org.apache.inlong.manager.workflow.model;

import com.google.common.collect.Lists;

import org.apache.inlong.manager.workflow.exception.WorkflowException;
import org.apache.inlong.manager.workflow.model.definition.Element;
import org.apache.inlong.manager.workflow.model.definition.Process;
import org.apache.inlong.manager.workflow.model.definition.ProcessForm;
import org.apache.inlong.manager.workflow.model.definition.Task;
import org.apache.inlong.manager.workflow.model.definition.TaskForm;
import org.apache.inlong.manager.workflow.model.instance.ProcessInstance;
import org.apache.inlong.manager.workflow.model.instance.TaskInstance;

import java.util.List;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Workflow Context
 */
@Slf4j
@Data
public class WorkflowContext implements Cloneable {

    private String applicant;

    private Process process;

    private ProcessForm processForm;

    private Element currentElement;

    private ProcessInstance processInstance;

    private List<TaskInstance> newTaskInstances = Lists.newArrayList();

    private ActionContext actionContext;

    public String getApplicant() {
        return applicant;
    }

    public WorkflowContext setApplicant(String applicant) {
        this.applicant = applicant;
        return this;
    }

    public Process getProcess() {
        return process;
    }

    public WorkflowContext setProcess(Process process) {
        this.process = process;
        return this;
    }

    public ProcessInstance getProcessInstance() {
        return processInstance;
    }

    public WorkflowContext setProcessInstance(
            ProcessInstance processInstance) {
        this.processInstance = processInstance;
        return this;
    }

    public ProcessForm getProcessForm() {
        return processForm;
    }

    public WorkflowContext setProcessForm(ProcessForm processForm) {
        this.processForm = processForm;
        return this;
    }

    public Element getCurrentElement() {
        return currentElement;
    }

    public WorkflowContext setCurrentElement(
            Element currentElement) {
        this.currentElement = currentElement;
        return this;
    }

    public ActionContext getActionContext() {
        return actionContext;
    }

    public WorkflowContext setActionContext(ActionContext actionContext) {
        this.actionContext = actionContext;
        return this;
    }

    public List<TaskInstance> getNewTaskInstances() {
        return newTaskInstances;
    }

    public WorkflowContext setNewTaskInstances(
            List<TaskInstance> newTaskInstances) {
        this.newTaskInstances = newTaskInstances;
        return this;
    }

    @Override
    public WorkflowContext clone() {
        try {
            WorkflowContext workflowContext = (WorkflowContext) super.clone();
            workflowContext.setProcess(process.clone());
            workflowContext.setCurrentElement(currentElement.clone());
            if (actionContext != null) {
                workflowContext.setActionContext(actionContext.clone());
            }
            return workflowContext;
        } catch (Exception e) {
            log.error("", e);
            throw new WorkflowException("workflow context clone failed" + this.getProcessInstance().getId());
        }
    }

    public static class ActionContext implements Cloneable {

        private Action action;
        private String operator;
        private String remark;
        private TaskForm form;
        private TaskInstance actionTaskInstance;
        private Task task;
        private List<String> transferToUsers;

        public Action getAction() {
            return action;
        }

        public ActionContext setAction(Action action) {
            this.action = action;
            return this;
        }

        public String getOperator() {
            return operator;
        }

        public ActionContext setOperator(String operator) {
            this.operator = operator;
            return this;
        }

        public String getRemark() {
            return remark;
        }

        public ActionContext setRemark(String remark) {
            this.remark = remark;
            return this;
        }

        public TaskForm getForm() {
            return form;
        }

        public ActionContext setForm(TaskForm form) {
            this.form = form;
            return this;
        }

        public TaskInstance getActionTaskInstance() {
            return actionTaskInstance;
        }

        public ActionContext setActionTaskInstance(
                TaskInstance actionTaskInstance) {
            this.actionTaskInstance = actionTaskInstance;
            return this;
        }

        public Task getTask() {
            return task;
        }

        public ActionContext setTask(Task task) {
            this.task = task;
            return this;
        }

        public List<String> getTransferToUsers() {
            return transferToUsers;
        }

        public ActionContext setTransferToUsers(List<String> transferToUsers) {
            this.transferToUsers = transferToUsers;
            return this;
        }

        @Override
        protected ActionContext clone() throws CloneNotSupportedException {
            ActionContext actionContext = (ActionContext) super.clone();
            if (task != null) {
                actionContext.setTask(task.clone());
            }
            return actionContext;
        }
    }
}
