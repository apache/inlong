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

package org.apache.inlong.manager.common.workflow;

import org.apache.inlong.manager.common.model.instance.EventLog;
import org.apache.inlong.manager.common.model.instance.ProcessInstance;
import org.apache.inlong.manager.common.model.instance.TaskInstance;
import org.apache.inlong.manager.common.model.view.EventLogQuery;
import org.apache.inlong.manager.common.model.view.ProcessDetail;
import org.apache.inlong.manager.common.model.view.ProcessQuery;
import org.apache.inlong.manager.common.model.view.ProcessSummaryQuery;
import org.apache.inlong.manager.common.model.view.ProcessSummaryView;
import org.apache.inlong.manager.common.model.view.TaskQuery;
import org.apache.inlong.manager.common.model.view.TaskSummaryQuery;
import org.apache.inlong.manager.common.model.view.TaskSummaryView;

import java.util.List;

/**
 * Process query service
 */
public interface QueryService {

    /**
     * Get an instance of a process sheet
     *
     * @param processInstId Process ID
     * @return Process single instance
     */
    ProcessInstance getProcessInstance(Integer processInstId);

    /**
     * Obtain the approval history according to the process ticket number
     *
     * @param processInstId Process ID
     * @return Approval history
     */
    List<TaskInstance> listApproveHistory(Integer processInstId);

    /**
     * Obtain task instance based on task ID
     *
     * @param taskInstId Task ID
     * @return Task instance
     */
    TaskInstance getTaskInstance(Integer taskInstId);

    /**
     * Query the list of process sheet
     *
     * @param processQuery Query conditions
     * @return  The list of the process sheet
     */
    List<ProcessInstance> listProcess(ProcessQuery processQuery);

    /**
     * Query task list
     *
     * @param taskQuery Query conditions
     * @return the list of task sheet
     */
    List<TaskInstance> listTask(TaskQuery taskQuery);

    /**
     * Process statistics
     *
     * @param query Query conditions
     * @return statistical results
     */
    ProcessSummaryView processSummary(ProcessSummaryQuery query);

    /**
     * Task statistics
     *
     * @param query Query conditions
     * @return statistical results
     */
    TaskSummaryView taskSummary(TaskSummaryQuery query);

    /**
     * Obtain the details of the application form according to the application form number
     *
     * @param processInstId Process ID
     * @param taskInstId    Task ID
     * @return Detail
     */
    ProcessDetail detail(Integer processInstId, Integer taskInstId);

    /**
     * Get event log based on ID
     *
     * @param id
     * @return
     */
    EventLog getEventLog(Integer id);

    /**
     * Query event logs based on conditions
     *
     * @param query Query conditions
     * @return the list of log
     */
    List<EventLog> listEventLog(EventLogQuery query);

}
