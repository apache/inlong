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

package org.apache.inlong.manager.plugin.flink;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.inlong.audit.AuditIdEnum;
import org.apache.inlong.audit.entity.FlowType;
import org.apache.inlong.manager.pojo.audit.AuditInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.StringJoiner;

import static java.lang.Math.ceil;
import static org.apache.inlong.audit.consts.OpenApiConstants.DEFAULT_API_MINUTES_PATH;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_AUDIT_CYCLE;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_AUDIT_ID;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_END_TIME;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_INLONG_GROUP_Id;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_INLONG_STREAM_Id;
import static org.apache.inlong.audit.consts.OpenApiConstants.PARAMS_START_TIME;
import static org.apache.inlong.manager.common.consts.InlongConstants.*;


/**
 *     This class is used to calculate the recommended parallelism based on the maximum message per second per core.
 *     The data volume is calculated based on the average data count per hour.
 *     The data count is retrieved from the inlong audit API.
 */
@Slf4j
@Component
public class FlinkParallelismOptimizer {

    @Value("${audit.query.url:http://127.0.0.1:10080}")
    public String auditQueryUrl;

    private static final int MAX_PARALLELISM = 2048;
    private long maximumMessagePerSecondPerCore = 1000L;
    private static final int DEFAULT_PARALLELISM = 1;
    private static final long DEFAULT_ERROR_DATA_VOLUME = 0L;
    private static final FlowType DEFAULT_FLOWTYPE = FlowType.OUTPUT;
    private static final String DEFAULT_AUDIT_TYPE = "DataProxy";
    private static final String AUDIT_CYCLE_REALTIME = "1";
    // maxmimum data scale counting range in hours
    private static final int DATA_SCALE_COUNTING_RANGE_IN_HOURS = 1;
    private static final String AUDIT_QUERY_DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"; //sample time format: 2024-08-23T22:47:38.866

    private static final String LOGTS_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String TIMEZONE_REGEX = "([+-])(\\d):";


    /**
     * Calculate recommended parallelism based on maximum message per second per core
     *
     * @return Recommended parallelism
     */
    public int calculateRecommendedParallelism(List<InlongStreamInfo> streamInfos) {
        long averageDataVolume;
        InlongStreamInfo streamInfo = streamInfos.get(0);
        try {
            averageDataVolume = getAverageDataVolume(streamInfo);
            log.info("Retrieved data volume: {}", averageDataVolume);
        } catch (Exception e) {
            log.error("Error retrieving data volume: {}", e.getMessage(), e);
            log.warn("Using default data volume: {}", DEFAULT_ERROR_DATA_VOLUME);
            averageDataVolume = DEFAULT_ERROR_DATA_VOLUME;
        }
        int newParallelism = (int) (averageDataVolume / maximumMessagePerSecondPerCore);
        newParallelism = Math.max(newParallelism, DEFAULT_PARALLELISM); // Ensure parallelism is at least the default value
        newParallelism = Math.min(newParallelism, MAX_PARALLELISM); // Ensure parallelism is at most MAX_PARALLELISM
        log.info("Calculated parallelism: {} for data volume: {}", newParallelism, averageDataVolume);
        return newParallelism;
    }

    /**
     * Initialize maximum message per second per core based on configuration
     *
     * @param maximumMessagePerSecondPerCore The maximum messages per second per core
     */
    public void setMaximumMessagePerSecondPerCore(Integer maximumMessagePerSecondPerCore) {
        if (maximumMessagePerSecondPerCore == null || maximumMessagePerSecondPerCore <= 0) {
            log.error("Illegal flink.maxpercore property, must be nonnull and positive, using default value: {}", maximumMessagePerSecondPerCore);
        } else {
            this.maximumMessagePerSecondPerCore = maximumMessagePerSecondPerCore;
        }
    }

    /**
     * Get average data volume on the scale specified by DATA_SCALE_COUNTING_RANGE_IN_HOURS
     *
     * @param streamInfo inlong stream info
     * @return The average data count per hour
     */
    private long getAverageDataVolume(InlongStreamInfo streamInfo) {
        // Since the audit module uses local time, we need to use ZonedDateTime to get the current time
        String dataTimeZone = streamInfo.getSourceList().get(0).getDataTimeZone();

        // This regex pattern matches a time zone offset in the format of "GMT+/-X:00"
        // where X is a single digit (e.g., "GMT+8:00"). The pattern captures the "+" or "-" sign
        // and the single digit, then it replaces the single digit with two digits by adding a "0" in front of it.
        // For example, "GMT+8:00" becomes "GMT+08:00" in order to match standard offset-based ZoneId.
        dataTimeZone = dataTimeZone.replaceAll(TIMEZONE_REGEX, "$10$2:");
        ZoneId dataZone = ZoneId.of(dataTimeZone);

        ZonedDateTime endTime = ZonedDateTime.now(dataZone);
        ZonedDateTime startTime = endTime.minusHours(DATA_SCALE_COUNTING_RANGE_IN_HOURS);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(AUDIT_QUERY_DATE_TIME_FORMAT);

        // counting data volume on with DATA_PROXY_OUTPUT auditId
        int auditId = AuditIdEnum.getAuditId(DEFAULT_AUDIT_TYPE, DEFAULT_FLOWTYPE).getValue();
        StringJoiner urlParameters = new StringJoiner(AMPERSAND)
                .add(PARAMS_START_TIME + EQUAL + startTime.format(formatter))
                .add(PARAMS_END_TIME + EQUAL + endTime.format(formatter))
                .add(PARAMS_INLONG_GROUP_Id + EQUAL + streamInfo.getInlongGroupId())
                .add(PARAMS_INLONG_STREAM_Id + EQUAL + streamInfo.getInlongStreamId())
                .add(PARAMS_AUDIT_ID + EQUAL + auditId)
                .add(PARAMS_AUDIT_CYCLE + EQUAL + AUDIT_CYCLE_REALTIME);

        String url = auditQueryUrl + DEFAULT_API_MINUTES_PATH + QUESTION_MARK + urlParameters;

        return getAverageDataVolumeFromAuditInfo(url);
    }

    /**
     * Request audit data from inlong audit API, parse the response and return the total count in the given time range.
     *
     * @param url The URL to request data from
     * @return The total count of the audit data
     */
    private long getAverageDataVolumeFromAuditInfo(String url) {
        log.debug("Requesting audit data from URL: {}", url);
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(url);
            try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
                return parseResponseAndCalculateAverageDataVolume(response);
            } catch (IOException e) {
                log.error("Error executing HTTP request to audit API: {}", url, e);
            }
        } catch (IOException e) {
            log.error("Error creating or closing HTTP client: {}", url, e);
        }
        return DEFAULT_ERROR_DATA_VOLUME;
    }

    /**
     * Parse the HTTP response and calculate the total count from the audit data.
     *
     * @param response The HTTP response
     * @return The total count of the audit data
     * @throws IOException If an I/O error occurs
     */
    private long parseResponseAndCalculateAverageDataVolume(CloseableHttpResponse response) throws IOException {
        HttpEntity entity = response.getEntity();
        if (entity == null) {
            log.warn("Empty response entity from audit API, returning default count.");
            return DEFAULT_ERROR_DATA_VOLUME;
        }

        String responseString = EntityUtils.toString(entity);
        log.debug("Flink dynamic parallelism optimizer got response from audit API: {}", responseString);

        JsonObject jsonObject = JsonParser.parseString(responseString).getAsJsonObject();
        AuditInfo[] auditInfoArray = new Gson().fromJson(jsonObject.getAsJsonArray("data"), AuditInfo[].class);

        ZonedDateTime minLogTs = null;
        ZonedDateTime maxLogTs = null;
        DateTimeFormatter logTsFormatter = DateTimeFormatter.ofPattern(LOGTS_DATE_TIME_FORMAT).withZone(ZoneId.systemDefault());
        long totalCount = 0L;
        for (AuditInfo auditData : auditInfoArray) {
            if (auditData != null) {
                ZonedDateTime logTs = ZonedDateTime.parse(auditData.getLogTs(), logTsFormatter);
                if (minLogTs == null || logTs.isBefore(minLogTs)) {
                    minLogTs = logTs;
                }
                if (maxLogTs == null || logTs.isAfter(maxLogTs)) {
                    maxLogTs = logTs;
                }
                log.debug("parsed AuditInfo, Count: {}, Size: {}", auditData.getCount(), auditData.getSize());
                totalCount += auditData.getCount();
            } else {
                log.error("Null AuditInfo found in response data.");
            }
        }

        if (minLogTs != null && maxLogTs != null) {
            long timeDifferenceInSeconds = maxLogTs.toEpochSecond() - minLogTs.toEpochSecond();
            log.info("Time difference in seconds: {}", timeDifferenceInSeconds);
            if (timeDifferenceInSeconds > 0) {
                return totalCount / timeDifferenceInSeconds;
            }
        }
        return DEFAULT_ERROR_DATA_VOLUME;
    }
}
