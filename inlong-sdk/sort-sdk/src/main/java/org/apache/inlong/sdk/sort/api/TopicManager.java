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
 *
 */

package org.apache.inlong.sdk.sort.api;

import org.apache.inlong.sdk.sort.entity.InLongTopic;

import java.util.Collection;
import java.util.Set;

public abstract class TopicManager implements Cleanable {
    protected ClientContext context;
    protected QueryConsumeConfig queryConsumeConfig;

    public TopicManager(ClientContext context, QueryConsumeConfig queryConsumeConfig) {
        this.context = context;
        this.queryConsumeConfig = queryConsumeConfig;
    }

    public abstract TopicFetcher addFetcher(InLongTopic inLongTopic);

    public abstract TopicFetcher removeFetcher(InLongTopic inLongTopic, boolean closeFetcher);

    public abstract TopicFetcher getFetcher(String fetchKey);

    public abstract Collection<TopicFetcher> getAllFetchers();

    public abstract Set<String> getManagedInLongTopics();

    public abstract void offlineAllTopicsAndPartitions();

    public abstract void close();
}
