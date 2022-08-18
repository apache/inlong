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
import org.apache.inlong.sdk.sort.impl.decode.MessageDeserializer;
import org.apache.inlong.sdk.sort.interceptor.MsgTimeInterceptor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public abstract class SingleTopicFetcher implements TopicFetcher {
    protected InLongTopic inLongTopic;
    protected ClientContext context;
    protected Deserializer deserializer;
    protected volatile Thread fetchThread;
    protected volatile boolean closed = false;
    protected volatile boolean isStopConsume = false;
    // use for empty topic to sleep
    protected long sleepTime = 0L;
    protected int emptyFetchTimes = 0;
    // for rollback
    protected Interceptor interceptor;
    protected Seeker seeker;

    public SingleTopicFetcher(
            InLongTopic inLongTopic,
            ClientContext context,
            Interceptor interceptor,
            Deserializer deserializer) {
        this.inLongTopic = inLongTopic;
        this.context = context;
        this.deserializer = Optional.ofNullable(deserializer).orElse(new MessageDeserializer());
        this.interceptor = Optional.ofNullable(interceptor).orElse(new MsgTimeInterceptor());
    }

    @Override
    public boolean updateTopic(InLongTopic topic) {
        if (Objects.equals(inLongTopic, topic)) {
            return false;
        }
        this.inLongTopic = topic;
        Optional.ofNullable(seeker).ifPresent(seeker -> seeker.configure(inLongTopic));
        Optional.ofNullable(interceptor).ifPresent(interceptor -> interceptor.configure(inLongTopic));
        return true;
    }

    @Deprecated
    @Override
    public boolean updateTopics(List<InLongTopic> topics) {
        if (topics.size() != 1) {
            return false;
        }
        return this.updateTopic(topics.get(0));
    }
}
