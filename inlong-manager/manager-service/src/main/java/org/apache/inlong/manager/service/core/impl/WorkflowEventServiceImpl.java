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

package org.apache.inlong.manager.service.core.impl;

import com.github.pagehelper.Page;
import com.github.pagehelper.PageHelper;

import org.apache.inlong.manager.common.beans.PageResult;
import org.apache.inlong.manager.common.util.PageUtils;
import org.apache.inlong.manager.service.core.WorkflowEventService;
import org.apache.inlong.manager.workflow.core.EventListenerService;
import org.apache.inlong.manager.workflow.core.QueryService;
import org.apache.inlong.manager.workflow.core.event.process.ProcessEvent;
import org.apache.inlong.manager.workflow.core.event.task.TaskEvent;
import org.apache.inlong.manager.workflow.model.instance.EventLog;
import org.apache.inlong.manager.workflow.model.view.EventLogQuery;
import org.apache.inlong.manager.workflow.model.view.EventLogView;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Workflow event related services
 *
 */
@Service
public class WorkflowEventServiceImpl implements WorkflowEventService {

    @Autowired
    private QueryService queryService;

    @Autowired
    private EventListenerService eventListenerService;

    @Override
    public EventLogView get(Integer id) {
        return EventLogView.fromEventLog(queryService.getEventLog(id), true);
    }

    @Override
    public PageResult<EventLogView> list(EventLogQuery query) {
        PageHelper.startPage(query.getPageNum(), query.getPageSize());
        Page<EventLog> page = (Page<EventLog>) queryService.listEventLog(query);

        return PageUtils.getPageResult(page, log -> EventLogView.fromEventLog(log, false));
    }

    @Override
    public void executeEventListener(Integer eventLogId) {
        eventListenerService.executeEventListener(eventLogId);
    }

    @Override
    public void executeProcessEventListener(Integer processInstId, String listenerName) {
        eventListenerService.executeProcessEventListener(processInstId, listenerName);
    }

    @Override
    public void executeTaskEventListener(Integer taskInstId, String listenerName) {
        eventListenerService.executeTaskEventListener(taskInstId, listenerName);
    }

    @Override
    public void triggerProcessEvent(Integer processInstId, ProcessEvent processEvent) {
        eventListenerService.triggerProcessEvent(processInstId, processEvent);
    }

    @Override
    public void triggerTaskEvent(Integer taskInstId, TaskEvent taskEvent) {
        eventListenerService.triggerTaskEvent(taskInstId, taskEvent);
    }
}
