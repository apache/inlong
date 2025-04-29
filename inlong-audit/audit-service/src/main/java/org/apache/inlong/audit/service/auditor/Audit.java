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

package org.apache.inlong.audit.service.auditor;

import org.apache.inlong.audit.Exception.InvalidRequestException;
import org.apache.inlong.audit.service.cache.AbstractCache;
import org.apache.inlong.audit.service.cache.HalfHourCache;
import org.apache.inlong.audit.service.cache.HourCache;
import org.apache.inlong.audit.service.cache.RealTimeQuery;
import org.apache.inlong.audit.service.cache.TenMinutesCache;
import org.apache.inlong.audit.service.entities.AuditCycle;
import org.apache.inlong.audit.service.entities.StatData;
import org.apache.inlong.audit.service.metric.MetricsManager;
import org.apache.inlong.audit.service.utils.AuditUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static org.apache.inlong.audit.consts.ConfigConstants.DEFAULT_AUDIT_TAG;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_HTTP_BODY_DATA;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_HTTP_BODY_ERR_MSG;
import static org.apache.inlong.audit.consts.OpenApiConstants.KEY_HTTP_BODY_SUCCESS;
import static org.apache.inlong.audit.service.entities.AuditCycle.DAY;
import static org.apache.inlong.audit.service.entities.AuditCycle.HOUR;
import static org.apache.inlong.audit.service.entities.AuditCycle.MINUTE_10;
import static org.apache.inlong.audit.service.entities.AuditCycle.MINUTE_30;

public class Audit {

    private static final Logger LOGGER = LoggerFactory.getLogger(Audit.class);
    private static final Gson GSON = new Gson();
    private static volatile Audit instance = null;

    private Audit() {
    }

    public static Audit getInstance() {
        if (instance == null) {
            synchronized (Audit.class) {
                if (instance == null) {
                    instance = new Audit();
                }
            }
        }
        return instance;
    }

    public JsonObject getData(String requestInfo) {
        try {
            RequestInfo request = GSON.fromJson(requestInfo, RequestInfo.class);
            AuditCycle auditCycle = AuditUtils.getAuditCycleTime(request.getStartTime(), request.getEndTime());
            validateRequest(request, auditCycle);

            ReconciliationData data = getAuditData(request, auditCycle);
            return createResponseJson(true, data, null);

        } catch (InvalidRequestException e) {
            LOGGER.error("Invalid request parameters: {}", e.getMessage());
            return createResponseJson(false, null, e.getMessage());
        } catch (Exception e) {
            LOGGER.error("Failed to process reconciliation request", e);
            return createResponseJson(false, null,
                    "Internal server error: " + e.getMessage());
        }
    }

    private ReconciliationData getAuditData(RequestInfo request, AuditCycle auditCycle) {
        // First get the data from the cache
        ReconciliationData data = getDataFromCache(request, auditCycle);
        if (data != null && data.isNotEmpty() && data.getDiffRatio() <= request.getDiffRatio()) {
            return data;
        }

        long statTimeMillis = System.currentTimeMillis();

        // Second, query the data from the data storage (without deduplication)
        data = getDataFromStorage(request, false);
        if (data.getDiffRatio() <= request.getDiffRatio()) {
            return data;
        }

        // Finally, query the data from the data storage (to deduplicate the data)
        data = getDataFromStorage(request, true);
        MetricsManager.getInstance().addApiMetricNoCache(auditCycle,
                System.currentTimeMillis() - statTimeMillis);
        LOGGER.info("Get audit data from data storage by distinct. Request info: {}", request);
        return data;
    }

    private void validateRequest(RequestInfo request, AuditCycle auditCycle) throws InvalidRequestException {
        if (!areIdsValid(request) || !isAuditCycleValid(auditCycle)) {
            throw new InvalidRequestException("Invalid parameters: " + request);
        }
        setDefaultAuditTagIfBlank(request);
    }

    private void setDefaultAuditTagIfBlank(RequestInfo request) {
        if (StringUtils.isBlank(request.getSrcAuditTag())) {
            request.setSrcAuditTag(DEFAULT_AUDIT_TAG);
        }
        if (StringUtils.isBlank(request.getDestAuditTag())) {
            request.setDestAuditTag(DEFAULT_AUDIT_TAG);
        }
    }

    private boolean areIdsValid(RequestInfo request) {
        return Objects.nonNull(request.getInlongGroupId()) &&
                Objects.nonNull(request.getInlongStreamId()) &&
                Objects.nonNull(request.getSrcAuditId()) &&
                Objects.nonNull(request.getDestAuditId());
    }

    private boolean isAuditCycleValid(AuditCycle auditCycle) {
        return Arrays.asList(
                MINUTE_10,
                MINUTE_30,
                HOUR,
                DAY).contains(auditCycle);
    }

    private JsonObject createResponseJson(boolean isSuccess, ReconciliationData auditData, String errorMessage) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(KEY_HTTP_BODY_SUCCESS, isSuccess);
        jsonObject.addProperty(KEY_HTTP_BODY_ERR_MSG, errorMessage);
        jsonObject.add(KEY_HTTP_BODY_DATA,
                GSON.toJsonTree(auditData != null ? auditData.getCombinedData() : new LinkedList<>()));
        return jsonObject;
    }

    private ReconciliationData getDataFromCache(RequestInfo request, AuditCycle auditCycle) {
        AbstractCache auditCache = getAuditCache(auditCycle);
        if (auditCache == null) {
            return null;
        }
        List<StatData> srcData = auditCache.getData(request.getStartTime(), request.getEndTime(),
                request.getInlongGroupId(), request.getInlongStreamId(), request.getSrcAuditId(),
                request.getSrcAuditTag(), false);
        List<StatData> destData = auditCache.getData(request.getStartTime(), request.getEndTime(),
                request.getInlongGroupId(), request.getInlongStreamId(), request.getDestAuditId(),
                request.getDestAuditTag(), false);
        return new ReconciliationData(AuditUtils.mergeStatDataList(srcData), AuditUtils.mergeStatDataList(destData));
    }

    private ReconciliationData getDataFromStorage(RequestInfo request, boolean needDistinct) {
        List<StatData> srcData = RealTimeQuery.getInstance().queryAuditData(request.getStartTime(),
                request.getEndTime(), request.getInlongGroupId(),
                request.getInlongStreamId(), request.getSrcAuditId(), request.getSrcAuditTag(), needDistinct);
        List<StatData> destData = RealTimeQuery.getInstance().queryAuditData(request.getStartTime(),
                request.getEndTime(), request.getInlongGroupId(),
                request.getInlongStreamId(), request.getDestAuditId(), request.getDestAuditTag(), needDistinct);
        return new ReconciliationData(AuditUtils.mergeStatDataList(srcData), AuditUtils.mergeStatDataList(destData));
    }

    private AbstractCache getAuditCache(AuditCycle auditCycle) {
        switch (auditCycle) {
            case MINUTE_10:
                return TenMinutesCache.getInstance();
            case MINUTE_30:
                return HalfHourCache.getInstance();
            case HOUR:
            case DAY:
                return HourCache.getInstance();
            default:
                return null;
        }
    }
}
