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

package org.apache.inlong.manager.service.workflow;

import com.github.pagehelper.PageInfo;
import java.util.List;
import org.apache.inlong.manager.common.model.definition.ProcessForm;
import org.apache.inlong.manager.common.model.definition.TaskForm;
import org.apache.inlong.manager.common.model.view.ProcessDetail;
import org.apache.inlong.manager.common.model.view.ProcessListView;
import org.apache.inlong.manager.common.model.view.ProcessQuery;
import org.apache.inlong.manager.common.model.view.ProcessSummaryQuery;
import org.apache.inlong.manager.common.model.view.ProcessSummaryView;
import org.apache.inlong.manager.common.model.view.TaskListView;
import org.apache.inlong.manager.common.model.view.TaskQuery;
import org.apache.inlong.manager.common.model.view.TaskSummaryQuery;
import org.apache.inlong.manager.common.model.view.TaskSummaryView;
import org.apache.inlong.manager.common.pojo.workflow.WorkflowResult;

/**
 * Workflow service
 */
public interface WorkflowService {

    /**
     * Initiation process
     *
     * @param process Process name
     * @param applicant Applicant
     * @param form Process form
     * @return result
     */
    WorkflowResult start(ProcessName process, String applicant, ProcessForm form);

    /**
     * Cancellation process application
     *
     * @param processInstId Process instance ID
     * @param operator Operator
     * @param remark Remarks information
     * @return result
     */
    WorkflowResult cancel(Integer processInstId, String operator, String remark);

    /**
     * Approval and consent
     *
     * @param taskId Task ID
     * @param form Form information
     * @param operator Operator
     * @return result
     */
    WorkflowResult approve(Integer taskId, String remark, TaskForm form, String operator);

    /**
     * reject
     *
     * @param taskId Task ID
     * @param remark Remarks information
     * @param operator Operator
     * @return result
     */
    WorkflowResult reject(Integer taskId, String remark, String operator);

    /**
     * Change approver
     *
     * @param taskId Task ID
     * @param remark Remarks
     * @param to Transfer to
     * @param operator Operator
     * @return result
     */
    WorkflowResult transfer(Integer taskId, String remark, List<String> to, String operator);

    /**
     * Complete task-true to automatic task
     *
     * @param taskId System Task ID
     * @param remark Remarks
     * @param operator Operator
     * @return result
     */
    WorkflowResult complete(Integer taskId, String remark, String operator);

    /**
     * Query process details according to the tracking number
     *
     * @param processInstId Process form number
     * @param taskInstId Task ID of the operation-nullable
     * @return Detail
     */
    ProcessDetail detail(Integer processInstId, Integer taskInstId);

    /**
     * Get a list of bills
     *
     * @param query Query conditions
     * @return List
     */
    PageInfo<ProcessListView> listProcess(ProcessQuery query);

    /**
     * Get task list
     *
     * @param query Query conditions
     * @return List
     */
    PageInfo<TaskListView> listTask(TaskQuery query);

    /**
     * Get process statistics
     *
     * @param query Query conditions
     * @return Statistical data
     */
    ProcessSummaryView processSummary(ProcessSummaryQuery query);

    /**
     * Get task statistics
     *
     * @param query Query conditions
     * @return Statistical data
     */
    TaskSummaryView taskSummary(TaskSummaryQuery query);

    /**
     * Get task execution log
     *
     * @param query Query conditions
     * @return Execution log
     */
    PageInfo<WorkflowTaskExecuteLog> listTaskExecuteLogs(WorkflowTaskExecuteLogQuery query);

}
