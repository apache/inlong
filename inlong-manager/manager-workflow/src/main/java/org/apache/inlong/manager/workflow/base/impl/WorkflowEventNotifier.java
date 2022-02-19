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

package org.apache.inlong.manager.workflow.base.impl;

import org.apache.inlong.manager.dao.mapper.WorkflowEventLogEntityMapper;
import org.apache.inlong.manager.workflow.event.EventListenerManagerFactory;
import org.apache.inlong.manager.workflow.event.process.ProcessEventNotifier;
import org.apache.inlong.manager.workflow.event.task.TaskEventNotifier;

/**
 * Workflow event notifier
 */
public class WorkflowEventNotifier {

    private final ProcessEventNotifier processEventNotifier;

    private final TaskEventNotifier taskEventNotifier;

    public WorkflowEventNotifier(EventListenerManagerFactory factory) {
        WorkflowEventLogEntityMapper eventLogMapper = factory.getEventLogMapper();
        taskEventNotifier = new TaskEventNotifier(factory.getTaskListenerManager(), eventLogMapper);
        processEventNotifier = new ProcessEventNotifier(factory.getProcessListenerManager(), eventLogMapper);
    }

    public ProcessEventNotifier getProcessEventNotifier() {
        return processEventNotifier;
    }

    public TaskEventNotifier getTaskEventNotifier() {
        return taskEventNotifier;
    }

}
