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
import org.apache.inlong.sdk.sort.api.InLongTopicManager;
import org.apache.inlong.sdk.sort.api.QueryConsumeConfig;
import org.apache.inlong.sdk.sort.api.SortClientConfig.TopicManagerType;

public class InLongTopicManagerFactory {

    public static InLongTopicManager createInLongTopicManager(
            TopicManagerType type,
            ClientContext context,
            QueryConsumeConfig queryConsumeConfig) {
        switch (type) {
            case single_topic: return createSingleTopicManager(context, queryConsumeConfig);
            case multi_topic: return createMultiTopicManager(context, queryConsumeConfig);
            default: return createSingleTopicManager(context, queryConsumeConfig);
        }
    }

    public static InLongTopicManager createSingleTopicManager(
            ClientContext context,
            QueryConsumeConfig queryConsumeConfig) {
        return new InLongSingleTopicManager(context, queryConsumeConfig);
    }

    public static InLongTopicManager createMultiTopicManager(
            ClientContext context,
            QueryConsumeConfig queryConsumeConfig) {
        return null;
    }
}
