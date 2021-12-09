/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.sort.impl;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.inlong.sort.api.Cleanable;
import org.apache.inlong.sort.api.ClientContext;
import org.apache.inlong.sort.api.InLongTopicChangeListener;
import org.apache.inlong.sort.entity.InLongTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssignmentsListenerExecutor implements Cleanable {

    private final String logPrefix = "[" + AssignmentsListenerExecutor.class.getSimpleName() + "] ";
    private final Logger logger = LoggerFactory.getLogger(AssignmentsListenerExecutor.class);
    private final ClientContext context;
    private final ThreadPoolExecutor executor;
    private final InLongTopicChangeListener listener;

    public AssignmentsListenerExecutor(ClientContext context, int threadSize, int queueSize) {
        this.context = context;
        this.listener = context.getConfig().getAssignmentsListener();
        this.executor = new ThreadPoolExecutor(threadSize, threadSize,
                60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(queueSize));
    }

    /**
     * callback sort
     *
     * @param taskId String
     * @param allPartitionIds Set InLongTopic
     * @param newPartitionIds Set InLongTopic
     * @param removedPartitionIds Set InLongTopic
     * @return true/false
     */
    public Future<Boolean> call(final String taskId, final Set<InLongTopic> allPartitionIds,
            final Set<InLongTopic> newPartitionIds,
            final Set<InLongTopic> removedPartitionIds) {

        Callable<Boolean> task = new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {

                boolean result = false;

                try {
                    result = listener.onAssignmentsChange(allPartitionIds, newPartitionIds,
                            removedPartitionIds);
                    return result;
                } catch (Exception e) {
                    logger.error(logPrefix + "|" + taskId + "|"
                            + context.getConfig().getSortTaskId()
                            + "|run error.", e);
                    throw e;
                }
            }
        };

        return executor.submit(task);
    }

    @Override
    public boolean clean() {
        if (executor != null) {
            executor.shutdown();
        }
        return true;
    }

}
