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

package org.apache.inlong.manager.service.cluster.tag.topic;

import org.apache.inlong.manager.pojo.cluster.tag.topic.ClusterTagChangeRetentionRequest;
import org.apache.inlong.manager.pojo.cluster.tag.topic.ClusterTagCreateTopicRequest;
import org.apache.inlong.manager.pojo.cluster.tag.topic.ClusterTagDeleteTopicRequest;

/**
 * Interface of the inlong message queue cluster operator.
 */
public interface ClusterTagService {

    /**
     * Create a topic in all message queue of Cluster Tag
     */
    String createTopic(ClusterTagCreateTopicRequest request);

    /**
     * Delete a topic from all message queue of Cluster Tag
     */
    String deleteTopic(ClusterTagDeleteTopicRequest request);

    /**
     * Change retention policies about all topics of this cluster tag
     */
    String changeRetention(ClusterTagChangeRetentionRequest request);
}
