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

package org.apache.inlong.manager.service.cluster.queue;

import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueClearTopicRequest;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueControlRequest;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueOfflineRequest;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueOnlineRequest;
import org.apache.inlong.manager.pojo.cluster.queue.MessageQueueSynchronizeTopicRequest;

/**
 * Interface of the inlong message queue cluster operator.
 */
public interface MessageQueueService {

    /**
     * Control produce operation and consume operation of Inlong message queue cluster 
     */
    void control(MessageQueueControlRequest request);

    /**
     * Build relationships between DataProxy cluster and MessageQueue cluster
     */
    void online(MessageQueueOnlineRequest request);

    /**
     * Remove relationships between DataProxy cluster and MessageQueue cluster
     */
    void offline(MessageQueueOfflineRequest request);

    /**
     * Synchronize all topic from cluster tag to message queue cluster
     */
    void synchronizeTopic(MessageQueueSynchronizeTopicRequest request);

    /**
     * Clear all topic from a message queue cluster
     */
    void clearTopic(MessageQueueClearTopicRequest request);
}
