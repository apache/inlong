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
import com.github.pagehelper.PageInfo;
import java.util.List;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.service.core.WorkflowEventService;
import org.apache.inlong.manager.common.workflow.EventListenerService;
import org.apache.inlong.manager.common.workflow.QueryService;
import org.apache.inlong.manager.common.event.process.ProcessEvent;
import org.apache.inlong.manager.common.event.task.TaskEvent;
import org.apache.inlong.manager.common.model.instance.EventLog;
import org.apache.inlong.manager.common.model.view.EventLogQuery;
import org.apache.inlong.manager.common.model.view.EventLogView;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Workflow event related services
 */
@Service
public class WorkflowEventServiceImpl implements WorkflowEventService {

    @Autowired
    private QueryService queryService;

    @Autowired
    private EventListenerService eventListenerService;

    @Override
    public EventLogView get(Integer id) {
        EventLog eventLog = queryService.getEventLog(id);
        return CommonBeanUtils.copyProperties(eventLog, EventLogView::new);
    }

    @Override
    public PageInfo<EventLogView> list(EventLogQuery query) {
        PageHelper.startPage(query.getPageNum(), query.getPageSize());
        Page<EventLog> page = (Page<EventLog>) queryService.listEventLog(query);

        List<EventLogView> viewList = CommonBeanUtils.copyListProperties(page, EventLogView::new);
        PageInfo<EventLogView> pageInfo = new PageInfo<>(viewList);
        pageInfo.setTotal(page.getTotal());

        return pageInfo;
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
