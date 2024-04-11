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

package org.apache.inlong.audit.cache;

import org.apache.inlong.audit.config.Configuration;
import org.apache.inlong.audit.entities.AuditCycle;
import org.apache.inlong.audit.entities.StatData;
import org.apache.inlong.audit.source.JdbcSource;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_API_CACHE_EXPIRED_HOURS;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_API_CACHE_MAX_SIZE;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_CACHE_EXPIRED_HOURS;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_CACHE_MAX_SIZE;

/**
 * Abstract cache.
 */
public class AbstractCache {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcSource.class);
    protected final Cache<String, StatData> cache;
    protected final ScheduledExecutorService monitorTimer = Executors.newSingleThreadScheduledExecutor();
    protected AuditCycle auditCycle;
    private static final int DEFAULT_MONITOR_INTERVAL = 1;

    protected AbstractCache(AuditCycle auditCycle) {
        cache = Caffeine.newBuilder()
                .maximumSize(Configuration.getInstance().get(KEY_API_CACHE_MAX_SIZE,
                        DEFAULT_API_CACHE_MAX_SIZE))
                .expireAfterWrite(Configuration.getInstance().get(KEY_API_CACHE_EXPIRED_HOURS,
                        DEFAULT_API_CACHE_EXPIRED_HOURS), TimeUnit.HOURS)
                .build();
        this.auditCycle = auditCycle;
        monitorTimer.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                monitor();
            }
        }, 0, DEFAULT_MONITOR_INTERVAL, TimeUnit.MINUTES);
    }

    /**
     * Get cache
     *
     * @return
     */
    public Cache<String, StatData> getCache() {
        return cache;
    }

    /**
     * Get data
     *
     * @param key
     * @return
     */
    public List<StatData> getData(String key) {
        StatData statData = cache.getIfPresent(key);
        if (null == statData) {
            return new LinkedList<>();
        }
        return Arrays.asList(statData);
    }

    /**
     * Destroy
     */
    public void destroy() {
        cache.cleanUp();
        monitorTimer.shutdown();
    }

    /**
     * Monitor
     */
    private void monitor() {
        LOG.info("{} api local cache size={}", auditCycle, cache.estimatedSize());
    }
}
