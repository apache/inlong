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

package org.apache.inlong.manager.workflow.core.event;

import java.util.Date;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.NetworkUtils;
import org.apache.inlong.manager.workflow.dao.EventLogStorage;
import org.apache.inlong.manager.workflow.exception.WorkflowListenerException;
import org.apache.inlong.manager.workflow.model.EventState;
import org.apache.inlong.manager.workflow.model.WorkflowContext;
import org.apache.inlong.manager.workflow.model.definition.Element;
import org.apache.inlong.manager.workflow.model.instance.EventLog;
import org.apache.inlong.manager.workflow.model.instance.ProcessInstance;

/**
 * Event listener with logging function
 */
@Slf4j
public abstract class LogableEventListener<EventType extends WorkflowEvent> implements EventListener<EventType> {

    private EventListener<EventType> eventListener;
    private EventLogStorage eventLogStorage;

    public LogableEventListener(EventListener<EventType> eventListener, EventLogStorage eventLogStorage) {
        this.eventListener = eventListener;
        this.eventLogStorage = eventLogStorage;
    }

    @Override
    public EventType event() {
        return eventListener.event();
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        if (eventListener.ignoreRecordLog()) {
            return executeListenerWithoutLog(context);
        }

        return executeListenerWithLog(context);
    }

    private ListenerResult executeListenerWithoutLog(WorkflowContext context) {
        EventLog eventLog = buildEventLog(context);
        try {
            ListenerResult result = eventListener.listen(context);
            log.debug("listener execute result:{} - {}", eventLog, result);
            return result;
        } catch (Exception e) {
            log.error("listener exception:{}", eventLog, e);
            if (!async()) {
                throw new WorkflowListenerException(e);
            }
            return ListenerResult.fail(e);
        }
    }

    private ListenerResult executeListenerWithLog(WorkflowContext context) {
        EventLog eventLog = buildEventLog(context);
        ListenerResult result;
        try {
            result = eventListener.listen(context);
            eventLog.setState(result.isSuccess() ? EventState.SUCCESS.getState() : EventState.FAILED.getState());
            eventLog.setRemark(result.getRemark());
            eventLog.setException(Optional.ofNullable(result.getException()).map(Exception::getMessage).orElse(null));
        } catch (Exception e) {
            eventLog.setState(EventState.FAILED.getState());
            eventLog.setException(e.getMessage());
            log.error("listener exception:{}", JsonUtils.toJson(eventLog), e);
            if (!async()) {
                throw new WorkflowListenerException(e.getMessage());
            }
            result = ListenerResult.fail(e);
        } finally {
            eventLog.setEndTime(new Date());
            eventLogStorage.insert(eventLog);
        }
        return result;
    }

    @Override
    public boolean async() {
        return eventListener.async();
    }

    protected EventLog buildEventLog(WorkflowContext context) {
        ProcessInstance processInstance = context.getProcessInstance();
        Element currentElement = context.getCurrentElement();

        return new EventLog()
                .setProcessInstId(processInstance.getId())
                .setProcessName(processInstance.getName())
                .setProcessDisplayName(processInstance.getDisplayName())
                .setBusinessId(context.getProcessForm().getBusinessId())
                .setElementName(currentElement.getName())
                .setElementDisplayName(currentElement.getDisplayName())
                .setEventType(event().getClass().getSimpleName())
                .setEvent(event().name())
                .setListener(eventListener.name())
                .setState(EventState.EXECUTING.getState())
                .setAsync(async())
                .setIp(NetworkUtils.getLocalIp())
                .setStartTime(new Date());
    }

}
