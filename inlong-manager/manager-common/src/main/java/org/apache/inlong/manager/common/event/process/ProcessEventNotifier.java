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

package org.apache.inlong.manager.common.event.process;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.inlong.manager.common.event.EventListenerManager;
import org.apache.inlong.manager.common.event.EventListenerNotifier;
import org.apache.inlong.manager.common.event.LogableEventListener;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.model.definition.Process;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;

/**
 * Process event notifier
 *
 */
@Slf4j
public class ProcessEventNotifier implements EventListenerNotifier<ProcessEvent> {

    private final ExecutorService executorService = new ThreadPoolExecutor(
            20,
            20,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setNameFormat("async-process-event-notifier-%s").build(),
            new CallerRunsPolicy());

    private EventListenerManager<ProcessEvent, ProcessEventListener> eventListenerManager;

    public ProcessEventNotifier(ProcessEventListenerManager eventListenerManager) {
        this.eventListenerManager = eventListenerManager;
    }

    @Override
    public void notify(ProcessEvent event, WorkflowContext sourceContext) {
        final WorkflowContext context = sourceContext.clone();
        Process process = context.getProcess();

        eventListenerManager.syncListeners(event).forEach(syncLogableNotify(context));

        process.syncListeners(event).forEach(syncLogableNotify(context));

        eventListenerManager.asyncListeners(event).forEach(asyncLogableNotify(context));

        process.asyncListeners(event).forEach(asyncLogableNotify(context));
    }

    @Override
    public void notify(String listenerName, boolean forceSync, WorkflowContext sourceContext) {
        final WorkflowContext context = sourceContext.clone();
        Process process = context.getProcess();

        Optional.ofNullable(this.eventListenerManager.listener(listenerName))
                .ifPresent(logableNotify(forceSync, context));

        Optional.ofNullable(process.listener(listenerName))
                .ifPresent(logableNotify(forceSync, context));

    }

    private Consumer<ProcessEventListener> logableNotify(boolean forceSync, WorkflowContext context) {
        return listener -> {
            if (forceSync || !listener.async()) {
                syncLogableNotify(context).accept(listener);
                return;
            }

            asyncLogableNotify(context).accept(listener);
        };
    }

    private Consumer<ProcessEventListener> asyncLogableNotify(WorkflowContext context) {
        return listener -> executorService.execute(() -> logableEventListener(listener).listen(context));
    }

    private Consumer<ProcessEventListener> syncLogableNotify(WorkflowContext context) {
        return listener -> logableEventListener(listener).listen(context);
    }

    private LogableProcessEventListener logableEventListener(ProcessEventListener listener) {
        if (listener instanceof LogableEventListener) {
            return (LogableProcessEventListener) listener;
        }
        return new LogableProcessEventListener(listener, eventListenerManager.eventLogStorage());
    }
}
