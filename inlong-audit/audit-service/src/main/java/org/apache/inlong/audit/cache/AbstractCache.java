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
import org.apache.inlong.audit.utils.CacheUtils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.config.ConfigConstants.DATE_FORMAT;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_API_CACHE_EXPIRED_HOURS;
import static org.apache.inlong.audit.config.OpenApiConstants.DEFAULT_API_CACHE_MAX_SIZE;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_CACHE_EXPIRED_HOURS;
import static org.apache.inlong.audit.config.OpenApiConstants.KEY_API_CACHE_MAX_SIZE;
import static org.apache.inlong.audit.consts.ConfigConstants.DEFAULT_AUDIT_TAG;

/**
 * Abstract cache.
 */
public class AbstractCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractCache.class);
    protected final Cache<String, StatData> cache;
    protected final ScheduledExecutorService monitorTimer = Executors.newSingleThreadScheduledExecutor();
    protected AuditCycle auditCycle;
    private static final int DEFAULT_MONITOR_INTERVAL = 1;

    // According to the startTime and endTime of the request parameters, the maximum number of cache keys generated.
    private static final int MAX_CACHE_KEY_SIZE = 1440;

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
     *
     * @param startTime
     * @param endTime
     * @param inlongGroupId
     * @param inlongStreamId
     * @param auditId
     * @param auditTag
     * @return
     */
    public List<StatData> getData(String startTime, String endTime, String inlongGroupId,
            String inlongStreamId, String auditId, String auditTag) {
        List<StatData> result = new LinkedList<>();
        List<String> keyList = buildCacheKeyList(startTime, endTime, inlongGroupId,
                inlongStreamId, auditId, auditTag);
        for (String cacheKey : keyList) {
            StatData statData = cache.getIfPresent(cacheKey);
            if (null == statData) {
                // Compatible with scenarios where the auditTag openapi parameter can be empty.
                statData = cache.getIfPresent(cacheKey + DEFAULT_AUDIT_TAG);
            }
            if (null != statData) {
                result.add(statData);
            }
        }
        return result;
    }

    private List<String> buildCacheKeyList(String startTime, String endTime, String inlongGroupId,
            String inlongStreamId, String auditId, String auditTag) {
        List<String> keyList = new LinkedList<>();
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
            Date startDate = dateFormat.parse(startTime);
            Date endDate = dateFormat.parse(endTime);
            for (int index = 0; index < MAX_CACHE_KEY_SIZE; index++) {
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(startDate);
                calendar.add(Calendar.MINUTE, index * auditCycle.getValue());
                calendar.set(Calendar.SECOND, 0);
                if (calendar.getTime().compareTo(endDate) > 0) {
                    break;
                }
                String time = dateFormat.format(calendar.getTime());
                keyList.add(CacheUtils.buildCacheKey(time, inlongGroupId, inlongStreamId, auditId, auditTag));
            }
        } catch (Exception exception) {
            LOGGER.error("It has exception when build cache key list!", exception);
        }
        return keyList;
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
        LOGGER.info("{} api local cache size={}", auditCycle, cache.estimatedSize());
    }
}
