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

package org.apache.inlong.manager.service.workflow.listener;

import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.service.mq.CreatePulsarSubscriptionTaskListener;
import org.apache.inlong.manager.service.mq.CreatePulsarTopicTaskListener;
import org.apache.inlong.manager.service.mq.DeletePulsarTopicTaskListener;
import org.apache.inlong.manager.service.resource.StreamSinkResourceListener;
import org.apache.inlong.manager.service.sort.StreamSortConfigListener;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.definition.ServiceTaskListenerProvider;
import org.apache.inlong.manager.workflow.definition.ServiceTaskType;
import org.apache.inlong.manager.workflow.event.task.QueueOperateListener;
import org.apache.inlong.manager.workflow.event.task.SinkOperateListener;
import org.apache.inlong.manager.workflow.event.task.SortOperateListener;
import org.apache.inlong.manager.workflow.event.task.SourceOperateListener;
import org.apache.inlong.manager.workflow.plugin.Plugin;
import org.apache.inlong.manager.workflow.plugin.PluginBinder;
import org.apache.inlong.manager.workflow.plugin.ProcessPlugin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

@Data
@Component
public class StreamTaskListenerFactory implements PluginBinder, ServiceTaskListenerProvider {

    private List<SourceOperateListener> sourceOperateListeners;

    private List<QueueOperateListener> queueOperateListeners;

    private List<SortOperateListener> sortOperateListeners;

    private List<SinkOperateListener> sinkOperateListeners;

    @Autowired
    private CreatePulsarTopicTaskListener createPulsarTopicTaskListener;
    @Autowired
    private CreatePulsarSubscriptionTaskListener createPulsarSubscriptionTaskListener;
    @Autowired
    private DeletePulsarTopicTaskListener deletePulsarTopicTaskListener;
    @Autowired
    private StreamSortConfigListener streamSortConfigListener;
    @Autowired
    private StreamSinkResourceListener sinkResourceListener;

    @PostConstruct
    public void init() {
        sourceOperateListeners = new LinkedList<>();
        queueOperateListeners = new LinkedList<>();
        queueOperateListeners.add(createPulsarTopicTaskListener);
        queueOperateListeners.add(createPulsarSubscriptionTaskListener);
        queueOperateListeners.add(deletePulsarTopicTaskListener);
        sortOperateListeners = new LinkedList<>();
        sortOperateListeners.add(streamSortConfigListener);
        sinkOperateListeners = new LinkedList<>();
        sinkOperateListeners.add(sinkResourceListener);
    }

    @Override
    public Iterable get(WorkflowContext workflowContext, ServiceTaskType serviceTaskType) {
        switch (serviceTaskType) {
            case INIT_MQ:
            case DELETE_MQ:
                List<QueueOperateListener> queueOperateListeners = getQueueOperateListener(workflowContext);
                return Lists.newArrayList(queueOperateListeners);
            case INIT_SORT:
            case STOP_SORT:
            case RESTART_SORT:
            case DELETE_SORT:
                List<SortOperateListener> sortOperateListeners = getSortOperateListener(workflowContext);
                return Lists.newArrayList(sortOperateListeners);
            case INIT_SOURCE:
            case STOP_SOURCE:
            case RESTART_SOURCE:
            case DELETE_SOURCE:
                List<SourceOperateListener> sourceOperateListeners = getSourceOperateListener(workflowContext);
                return Lists.newArrayList(sourceOperateListeners);
            case INIT_SINK:
                List<SinkOperateListener> sinkOperateListeners = getSinkOperateListener(workflowContext);
                return Lists.newArrayList(sinkOperateListeners);
            default:
                throw new IllegalArgumentException(String.format("Unsupported ServiceTaskType %s", serviceTaskType));
        }
    }

    /**
     * Get data source operate listener list.
     */
    public List<SourceOperateListener> getSourceOperateListener(WorkflowContext context) {
        List<SourceOperateListener> listeners = new ArrayList<>();
        for (SourceOperateListener listener : sourceOperateListeners) {
            if (listener != null && listener.accept(context)) {
                listeners.add(listener);
            }
        }
        return listeners;
    }

    /**
     * Get queue operate listener list.
     */
    public List<QueueOperateListener> getQueueOperateListener(WorkflowContext context) {
        List<QueueOperateListener> listeners = new ArrayList<>();
        for (QueueOperateListener listener : queueOperateListeners) {
            if (listener != null && listener.accept(context)) {
                listeners.add(listener);
            }
        }
        return listeners;
    }

    /**
     * Get sort operate listener list.
     */
    public List<SortOperateListener> getSortOperateListener(WorkflowContext context) {
        List<SortOperateListener> listeners = new ArrayList<>();
        for (SortOperateListener listener : sortOperateListeners) {
            if (listener != null && listener.accept(context)) {
                listeners.add(listener);
            }
        }
        return listeners;
    }

    /**
     * Get sink operate listener list.
     */
    public List<SinkOperateListener> getSinkOperateListener(WorkflowContext context) {
        List<SinkOperateListener> listeners = new ArrayList<>();
        for (SinkOperateListener listener : sinkOperateListeners) {
            if (listener != null && listener.accept(context)) {
                listeners.add(listener);
            }
        }
        return listeners;
    }

    @Override
    public void acceptPlugin(Plugin plugin) {
        if (!(plugin instanceof ProcessPlugin)) {
            return;
        }
        ProcessPlugin processPlugin = (ProcessPlugin) plugin;
        List<SourceOperateListener> pluginSourceOperateListeners = processPlugin.createSourceOperateListeners();
        if (CollectionUtils.isNotEmpty(pluginSourceOperateListeners)) {
            sourceOperateListeners.addAll(processPlugin.createSourceOperateListeners());
        }
        List<QueueOperateListener> pluginQueueOperateListeners = processPlugin.createQueueOperateListeners();
        if (CollectionUtils.isNotEmpty(pluginQueueOperateListeners)) {
            queueOperateListeners.addAll(pluginQueueOperateListeners);
        }
        List<SortOperateListener> pluginSortOperateListeners = processPlugin.createSortOperateListeners();
        if (CollectionUtils.isNotEmpty(pluginSortOperateListeners)) {
            sortOperateListeners.addAll(pluginSortOperateListeners);
        }
        List<SinkOperateListener> pluginSinkOperateListeners = processPlugin.createSinkOperateListeners();
        if (CollectionUtils.isNotEmpty(pluginSinkOperateListeners)) {
            sinkOperateListeners.addAll(pluginSinkOperateListeners);
        }
    }
}
