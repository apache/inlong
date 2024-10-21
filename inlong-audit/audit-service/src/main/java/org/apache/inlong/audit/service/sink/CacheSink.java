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

package org.apache.inlong.audit.service.sink;

import org.apache.inlong.audit.service.channel.DataQueue;
import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.entities.StatData;
import org.apache.inlong.audit.service.utils.CacheUtils;

import com.github.benmanes.caffeine.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_QUEUE_PULL_TIMEOUT;
import static org.apache.inlong.audit.service.config.ConfigConstants.DEFAULT_SOURCE_DB_SINK_INTERVAL;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_QUEUE_PULL_TIMEOUT;
import static org.apache.inlong.audit.service.config.ConfigConstants.KEY_SOURCE_DB_SINK_INTERVAL;

/**
 * Cache sink
 */
public class CacheSink implements AuditSink {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheSink.class);
    private final ScheduledExecutorService sinkTimer = Executors.newSingleThreadScheduledExecutor();
    private final DataQueue dataQueue;
    private final Cache<String, StatData> cache;
    private final int pullTimeOut;

    public CacheSink(DataQueue dataQueue, Cache<String, StatData> cache) {
        this.dataQueue = dataQueue;
        this.cache = cache;
        pullTimeOut = Configuration.getInstance().get(KEY_QUEUE_PULL_TIMEOUT,
                DEFAULT_QUEUE_PULL_TIMEOUT);
    }

    /**
     * start
     */
    public void start() {
        sinkTimer.scheduleWithFixedDelay(this::process,
                0,
                Configuration.getInstance().get(KEY_SOURCE_DB_SINK_INTERVAL,
                        DEFAULT_SOURCE_DB_SINK_INTERVAL),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Process
     */
    private void process() {
        try {
            StatData data = dataQueue.pull(pullTimeOut, TimeUnit.MILLISECONDS);
            while (data != null) {
                String cacheKey = CacheUtils.buildCacheKey(data.getLogTs(), data.getInlongGroupId(),
                        data.getInlongStreamId(), data.getAuditId(), data.getAuditTag());
                cache.put(cacheKey, data);
                data = dataQueue.pull(pullTimeOut, TimeUnit.MILLISECONDS);
            }
        } catch (Exception exception) {
            LOGGER.error("Process exception! ", exception);
        }
    }

    /**
     * Destroy
     */
    public void destroy() {
        sinkTimer.shutdown();
    }
}
