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

package org.apache.inlong.manager.service.listener;

import org.apache.inlong.manager.common.plugin.Plugin;
import org.apache.inlong.manager.common.plugin.PluginBinder;
import org.apache.inlong.manager.service.listener.queue.ClusterConfigListener;
import org.apache.inlong.manager.service.listener.queue.ClusterQueueResourceListener;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.definition.ServiceTaskType;
import org.apache.inlong.manager.workflow.definition.TaskListenerFactory;
import org.apache.inlong.manager.workflow.event.task.ClusterOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEventListener;

import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * The TaskEventListener factory for InlongCluster.
 */
@Component
public class ClusterTaskListenerFactory implements PluginBinder, TaskListenerFactory {

    private List<ClusterOperateListener> clusterOperateListeners;

    @Autowired
    private ClusterQueueResourceListener clusterQueueResourceListener;
    @Autowired
    private ClusterConfigListener clusterConfigListener;

    @PostConstruct
    public void init() {
        clusterOperateListeners = new LinkedList<>();
        clusterOperateListeners.add(clusterQueueResourceListener);
        clusterOperateListeners.add(clusterConfigListener);
    }

    @Override
    public void acceptPlugin(Plugin plugin) {
    }

    @Override
    public List<? extends TaskEventListener> get(WorkflowContext workflowContext, ServiceTaskType taskType) {
        switch (taskType) {
            case INIT_MQ:
                List<ClusterOperateListener> clusterOperateListeners = getQueueOperateListener(workflowContext);
                return Lists.newArrayList(clusterOperateListeners);
            default:
                throw new IllegalArgumentException(String.format("Unsupported ServiceTaskType %s", taskType));
        }
    }

    /**
     * Clear the list of listeners.
     */
    public void clearListeners() {
        clusterOperateListeners = new LinkedList<>();
    }

    /**
     * Get cluster queue operate listener list.
     */
    public List<ClusterOperateListener> getQueueOperateListener(WorkflowContext context) {
        List<ClusterOperateListener> listeners = new ArrayList<>();
        for (ClusterOperateListener listener : clusterOperateListeners) {
            if (listener != null && listener.accept(context)) {
                listeners.add(listener);
            }
        }
        return listeners;
    }

}
