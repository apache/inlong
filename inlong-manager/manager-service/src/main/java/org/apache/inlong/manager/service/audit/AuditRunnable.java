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

package org.apache.inlong.manager.service.audit;

import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.HttpUtils;
import org.apache.inlong.manager.pojo.audit.AuditInfo;
import org.apache.inlong.manager.pojo.audit.AuditRequest;
import org.apache.inlong.manager.pojo.audit.AuditResponse;
import org.apache.inlong.manager.pojo.audit.AuditVO;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Audit query Runnable
 */
public class AuditRunnable implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditRunnable.class);

    private static final String SECOND_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String DAY_FORMAT = "yyyy-MM-dd";
    private static final DateTimeFormatter SECOND_DATE_FORMATTER = DateTimeFormat.forPattern(SECOND_FORMAT);
    private static final DateTimeFormatter DAY_DATE_FORMATTER = DateTimeFormat.forPattern(DAY_FORMAT);

    private AuditRequest request;
    private String auditId;
    private String auditName;
    private List<AuditVO> auditVOList;
    private CountDownLatch latch;
    private RestTemplate restTemplate;
    private String auditUrl;
    private Map<String, String> auditIdMap;
    private Boolean listAll;

    public AuditRunnable(
            AuditRequest request,
            String auditId,
            String auditName,
            List<AuditVO> auditVOList,
            CountDownLatch latch,
            RestTemplate restTemplate,
            String auditUrl,
            Map<String, String> auditIdMap,
            Boolean listAll) {
        this.request = request;
        this.auditId = auditId;
        this.auditName = auditName;
        this.auditVOList = auditVOList;
        this.latch = latch;
        this.restTemplate = restTemplate;
        this.auditUrl = auditUrl;
        this.auditIdMap = auditIdMap == null ? new HashMap<>() : auditIdMap;
        this.listAll = listAll;
    }

    @Override
    public void run() {
        try {
            List<AuditInfo> auditSet;
            if (listAll) {
                auditSet = getAuditInfoListByIp(request, request.getInlongGroupId(), request.getInlongStreamId(),
                        request.getIp(), auditId);
            } else {
                auditSet = getAuditInfoList(request, request.getInlongGroupId(), request.getInlongStreamId(), auditId);
            }
            auditVOList.add(new AuditVO(auditId, auditName, auditSet, auditIdMap.getOrDefault(auditId, null)));
        } catch (Exception e) {
            LOGGER.error("query audit failed for request={}", request);
            throw new BusinessException("query audit failed");
        } finally {
            this.latch.countDown();
        }

    }

    private List<AuditInfo> getAuditInfoListByIp(AuditRequest request, String groupId, String streamId, String ip,
            String auditId) {
        List<AuditInfo> auditSet = new ArrayList<>();
        try {
            String start = request.getStartDate();
            String end = request.getEndDate();
            if (StringUtils.isBlank(streamId)) {
                streamId = "*";
            }
            if (StringUtils.isBlank(request.getEndDate())) {
                end = SECOND_DATE_FORMATTER.parseDateTime(request.getEndDate()).plusDays(1).toString(SECOND_FORMAT);
            }
            StringBuilder builder = new StringBuilder();
            builder.append(auditUrl);
            if (StringUtils.isNotBlank(ip)) {
                builder.append("/audit/query/getIds?")
                        .append("&ip=").append(ip);
            } else {
                builder.append("/audit/query/getIps?")
                        .append("&inlongGroupId=").append(groupId)
                        .append("&inlongStreamId=").append(streamId);
            }
            builder.append("&startTime=").append(start)
                    .append("&endTime=").append(end)
                    .append("&auditId=").append(auditId);
            String url = builder.toString();
            LOGGER.info("query audit url ={}", url);
            AuditResponse result = HttpUtils.request(restTemplate,
                    url,
                    HttpMethod.GET, null,
                    null,
                    AuditResponse.class);
            LOGGER.info("success to query audit info for url ={}", url);
            return CommonBeanUtils.copyListProperties(result.getData(), AuditInfo::new);
        } catch (Exception e) {
            LOGGER.info("query audit failed for request={}", request, e);
        }

        return auditSet;
    }

    private List<AuditInfo> getAuditInfoList(AuditRequest request, String groupId, String streamId, String auditId) {
        List<AuditInfo> auditSet = new ArrayList<>();
        try {
            if (StringUtils.isBlank(streamId)) {
                streamId = "*";
            }
            String start = DAY_DATE_FORMATTER.parseDateTime(request.getStartDate()).toString(SECOND_FORMAT);
            String end = DAY_DATE_FORMATTER.parseDateTime(request.getStartDate()).plusDays(1).toString(SECOND_FORMAT);
            if (StringUtils.isNotBlank(request.getEndDate())) {
                end = DAY_DATE_FORMATTER.parseDateTime(request.getEndDate()).plusDays(1).toString(SECOND_FORMAT);
            }
            StringBuilder builder = new StringBuilder();
            builder.append(auditUrl);
            switch (request.getTimeStaticsDim()) {
                case HOUR:
                    builder.append("/audit/query/hour?");
                    break;
                case DAY:
                    builder.append("/audit/query/day?");
                    break;
                default:
                    builder.append("/audit/query/minutes?");
                    break;
            }

            builder.append("startTime=").append(start)
                    .append("&endTime=").append(end)
                    .append("&auditId=").append(auditId)
                    .append("&inlongGroupId=").append(groupId)
                    .append("&inlongStreamId=").append(streamId)
                    .append("&auditCycle=1");
            String url = builder.toString();
            AuditResponse result = HttpUtils.request(restTemplate,
                    url,
                    HttpMethod.GET, null,
                    null,
                    AuditResponse.class);
            LOGGER.info("success to query audit info for url ={}", url);
            return CommonBeanUtils.copyListProperties(result.getData(), AuditInfo::new);
        } catch (Exception e) {
            LOGGER.info("query audit failed for request={}", request, e);
        }

        return auditSet;
    }
}