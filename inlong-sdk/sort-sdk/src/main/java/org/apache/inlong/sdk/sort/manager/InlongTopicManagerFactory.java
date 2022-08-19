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

package org.apache.inlong.sdk.sort.manager;

import org.apache.inlong.sdk.sort.api.ClientContext;
import org.apache.inlong.sdk.sort.api.InlongTopicManager;
import org.apache.inlong.sdk.sort.api.QueryConsumeConfig;
import org.apache.inlong.sdk.sort.api.SortClientConfig.TopicManagerType;

/**
 * Inlong topic manager factory.
 * To create single or multi topic fetcher manager according to the {@link TopicManagerType}
 */
public class InlongTopicManagerFactory {

    public static InlongTopicManager createInLongTopicManager(
            TopicManagerType type,
            ClientContext context,
            QueryConsumeConfig queryConsumeConfig) {
        switch (type) {
            case SINGLE_TOPIC: return createSingleTopicManager(context, queryConsumeConfig);
            case MULTI_TOPIC: return createMultiTopicManager(context, queryConsumeConfig);
            default: return createSingleTopicManager(context, queryConsumeConfig);
        }
    }

    public static InlongTopicManager createSingleTopicManager(
            ClientContext context,
            QueryConsumeConfig queryConsumeConfig) {
        return new InlongSingleTopicManager(context, queryConsumeConfig);
    }

    public static InlongTopicManager createMultiTopicManager(
            ClientContext context,
            QueryConsumeConfig queryConsumeConfig) {
        return null;
    }
}
