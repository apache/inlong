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

package org.apache.inlong.audit.service.cache;

import org.apache.inlong.audit.service.config.ConfigConstants;
import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.entities.AuditCycle;
import org.apache.inlong.audit.service.entities.CacheKeyEntity;
import org.apache.inlong.audit.service.entities.StatData;
import org.apache.inlong.audit.service.metric.MetricsManager;
import org.apache.inlong.audit.service.utils.CacheUtils;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.audit.consts.ConfigConstants.DEFAULT_AUDIT_TAG;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_CACHE_EXPIRED_HOURS;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_CACHE_MAX_SIZE;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_CACHE_EXPIRED_HOURS;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_API_CACHE_MAX_SIZE;

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

    private final DateTimeFormatter FORMATTER_YYMMDDHHMMSS = DateTimeFormatter.ofPattern(ConfigConstants.DATE_FORMAT);

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

    public List<StatData> getData(String startTime, String endTime, String inlongGroupId,
            String inlongStreamId, String auditId, String auditTag) {
        return getData(startTime, endTime, inlongGroupId, inlongStreamId, auditId, auditTag, true);

    }

    /**
     * @param startTime
     * @param endTime
     * @param inlongGroupId
     * @param inlongStreamId
     * @param auditId
     * @param auditTag
     * @return
     */
    public List<StatData> getData(String startTime, String endTime, String inlongGroupId,
            String inlongStreamId, String auditId, String auditTag, boolean needRetry) {
        List<StatData> result = new ArrayList<>();
        List<CacheKeyEntity> keyList = buildCacheKeyList(startTime, endTime, inlongGroupId,
                inlongStreamId, auditId, auditTag);

        for (CacheKeyEntity cacheKey : keyList) {
            StatData statData = fetchStatDataFromCache(cacheKey);
            if (statData == null && needRetry) {
                long statTimeMillis = System.currentTimeMillis();
                statData = fetchDataFromAuditStorage(cacheKey.getStartTime(), cacheKey.getEndTime(), inlongGroupId,
                        inlongStreamId, auditId, auditTag);
                MetricsManager.getInstance().addApiMetricNoCache(auditCycle,
                        System.currentTimeMillis() - statTimeMillis);
            }
            if (statData != null) {
                result.add(statData);
            }
        }
        return result;
    }

    private List<CacheKeyEntity> buildCacheKeyList(String startTime, String endTime, String inlongGroupId,
            String inlongStreamId, String auditId, String auditTag) {
        List<CacheKeyEntity> keyList = new LinkedList<>();
        try {
            LocalDateTime startDateTime = LocalDateTime.parse(startTime, FORMATTER_YYMMDDHHMMSS);
            LocalDateTime endDateTime = LocalDateTime.parse(endTime, FORMATTER_YYMMDDHHMMSS);
            LocalDateTime nowDateTime = LocalDateTime.now();
            LocalDateTime maxDateTime = endDateTime.isBefore(nowDateTime) ? endDateTime : nowDateTime;

            for (long index = 0; index < MAX_CACHE_KEY_SIZE; index++) {
                LocalDateTime currentDateTime = startDateTime.plusMinutes(index * auditCycle.getValue());
                if (!currentDateTime.isBefore(maxDateTime)) {
                    break;
                }
                String currentTime = currentDateTime.format(FORMATTER_YYMMDDHHMMSS);
                String cacheKey =
                        CacheUtils.buildCacheKey(currentTime, inlongGroupId, inlongStreamId, auditId, auditTag);
                keyList.add(new CacheKeyEntity(cacheKey, currentTime,
                        currentDateTime.plusMinutes(auditCycle.getValue()).format(FORMATTER_YYMMDDHHMMSS)));
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

    private StatData fetchDataFromAuditStorage(String startTime, String endTime, String inlongGroupId,
            String inlongStreamId,
            String auditId, String auditTag) {
        List<StatData> allStatData =
                RealTimeQuery.getInstance().queryLogTs(startTime, endTime, inlongGroupId, inlongStreamId, auditId);

        long totalCount = 0L;
        long totalSize = 0L;
        long totalDelay = 0L;

        for (StatData data : allStatData) {
            if (auditTag.equals(data.getAuditTag()) || auditTag.equals(DEFAULT_AUDIT_TAG) || auditTag.isEmpty()) {
                totalCount += data.getCount();
                totalSize += data.getSize();
                totalDelay += data.getDelay();
            }
        }

        StatData statData = new StatData();
        statData.setLogTs(startTime);
        statData.setInlongGroupId(inlongGroupId);
        statData.setInlongStreamId(inlongStreamId);
        statData.setAuditId(auditId);
        statData.setAuditTag(auditTag);
        statData.setCount(totalCount);
        statData.setSize(totalSize);
        statData.setDelay(totalDelay);
        return statData;
    }
    private StatData fetchStatDataFromCache(CacheKeyEntity cacheKey) {
        StatData statData = cache.getIfPresent(cacheKey.getCacheKey());
        if (statData == null) {
            // Compatible with scenarios where the auditTag openapi parameter can be empty.
            statData = cache.getIfPresent(cacheKey.getCacheKey() + DEFAULT_AUDIT_TAG);
        }
        return statData;
    }
}
