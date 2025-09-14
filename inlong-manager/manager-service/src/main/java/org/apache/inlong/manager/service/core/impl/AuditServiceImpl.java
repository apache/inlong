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

package org.apache.inlong.manager.service.core.impl;

import org.apache.inlong.audit.AuditOperator;
import org.apache.inlong.audit.entity.AuditInformation;
import org.apache.inlong.audit.entity.AuditProxy;
import org.apache.inlong.audit.entity.FlowType;
import org.apache.inlong.common.enums.IndicatorType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.HttpUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.pojo.audit.AuditProxyResponse;
import org.apache.inlong.manager.pojo.audit.AuditRequest;
import org.apache.inlong.manager.pojo.audit.AuditVO;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserRoleCode;
import org.apache.inlong.manager.service.audit.AuditRunnable;
import org.apache.inlong.manager.service.core.AuditService;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Audit service layer implementation
 */
@Lazy
@Service
public class AuditServiceImpl implements AuditService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditServiceImpl.class);

    // key 1: type of audit, like pulsar, hive, key 2: indicator type, value : entity of audit base item
    private final Map<String, Map<Integer, AuditInformation>> auditIndicatorMap = new ConcurrentHashMap<>();
    private final Map<String, String> auditItemMap = new ConcurrentHashMap<>();
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    // defaults to return all audit ids, can be overwritten in properties file
    // see audit id definitions: https://inlong.apache.org/docs/modules/audit/overview#audit-id
    @Value("#{'${audit.admin.ids:3,4,5,6}'.split(',')}")
    private List<String> auditIdListForAdmin;
    @Value("#{'${audit.user.ids:3,4,5,6}'.split(',')}")
    private List<String> auditIdListForUser;
    @Value("${audit.query.url:http://127.0.0.1:10080}")
    private String auditQueryUrl;

    @Autowired
    private StreamSinkEntityMapper sinkEntityMapper;
    @Autowired
    private StreamSourceEntityMapper sourceEntityMapper;
    @Autowired
    private InlongGroupEntityMapper inlongGroupMapper;
    @Autowired
    private RestTemplate restTemplate;

    @PostConstruct
    public void initialize() {
        LOGGER.info("init audit base item cache map for {}", AuditServiceImpl.class.getSimpleName());
        try {
            refreshBaseItemCache();
        } catch (Throwable t) {
            LOGGER.error("initialize audit base item cache error", t);
        }
    }

    @Override
    public Boolean refreshBaseItemCache() {
        LOGGER.debug("start to reload audit base item info");
        try {
            auditIndicatorMap.clear();
            List<AuditInformation> auditInformationList = AuditOperator.getInstance().getAllAuditInformation();
            List<AuditInformation> metricInformationList = AuditOperator.getInstance().getAllMetricInformation();
            List<AuditInformation> cdcMetricInformationList = AuditOperator.getInstance().getAllCdcIdInformation();
            cdcMetricInformationList.forEach(v -> {
                auditItemMap.put(String.valueOf(v.getAuditId()), v.getNameInChinese());
            });
            auditInformationList.forEach(v -> {
                auditItemMap.put(String.valueOf(v.getAuditId()), v.getNameInChinese());
            });
            metricInformationList.forEach(v -> {
                auditItemMap.put(String.valueOf(v.getAuditId()), v.getNameInChinese());
            });
        } catch (Throwable t) {
            LOGGER.error("failed to reload audit base item info", t);
            return false;
        }

        LOGGER.debug("success to reload audit base item info");
        return true;
    }

    @Override
    public String getAuditId(String type, IndicatorType indicatorType) {
        if (StringUtils.isBlank(type)) {
            return null;
        }
        Map<Integer, AuditInformation> itemMap = auditIndicatorMap.computeIfAbsent(type, v -> new HashMap<>());
        AuditInformation auditInformation = itemMap.get(indicatorType.getCode());
        if (auditInformation != null) {
            return String.valueOf(auditInformation.getAuditId());
        }
        FlowType flowType = indicatorType.getCode() % 2 == 0 ? FlowType.INPUT : FlowType.OUTPUT;
        auditInformation = AuditOperator.getInstance().buildAuditInformation(type, flowType,
                IndicatorType.isSuccessType(indicatorType),
                true,
                IndicatorType.isDiscardType(indicatorType),
                IndicatorType.isRetryType(indicatorType));
        Preconditions.expectNotNull(auditInformation, ErrorCodeEnum.AUDIT_ID_TYPE_NOT_SUPPORTED,
                String.format(ErrorCodeEnum.AUDIT_ID_TYPE_NOT_SUPPORTED.getMessage(), type));
        itemMap.put(indicatorType.getCode(), auditInformation);
        return String.valueOf(auditInformation.getAuditId());
    }

    @Override
    public List<AuditVO> listByCondition(AuditRequest request) throws Exception {
        LOGGER.info("begin query audit list request={}", request);
        Preconditions.expectNotNull(request, "request is null");

        String groupId = request.getInlongGroupId();
        String streamId = request.getInlongStreamId();

        // for now, we use the first sink type only.
        // this is temporary behavior before multiple sinks in one stream is fully supported.
        String sinkNodeType = null;
        String sourceNodeType = null;
        Integer sinkId = request.getSinkId();
        StreamSinkEntity sinkEntity = null;
        if (StringUtils.isNotBlank(streamId)) {
            List<StreamSinkEntity> sinkEntityList = sinkEntityMapper.selectByRelatedId(groupId, streamId);
            if (sinkId != null) {
                sinkEntity = sinkEntityMapper.selectByPrimaryKey(sinkId);
            } else if (CollectionUtils.isNotEmpty(sinkEntityList)) {
                sinkEntity = sinkEntityList.get(0);
            }
            // if sink info is existed, get sink type for query audit info.
            if (sinkEntity != null) {
                sinkNodeType = sinkEntity.getSinkType();
            }
        } else {
            sinkNodeType = request.getSinkType();
        }

        Map<String, String> auditIdMap = new HashMap<>();

        if (StringUtils.isNotBlank(groupId)) {
            InlongGroupEntity groupEntity = inlongGroupMapper.selectByGroupId(groupId);
            List<StreamSourceEntity> sourceEntityList = sourceEntityMapper.selectByRelatedId(groupId, streamId, null);
            if (CollectionUtils.isNotEmpty(sourceEntityList)) {
                sourceNodeType = sourceEntityList.get(0).getSourceType();
            }

            auditIdMap.put(getAuditId(sinkNodeType, IndicatorType.SEND_SUCCESS), sinkNodeType);

            if (CollectionUtils.isEmpty(request.getAuditIds())) {
                // properly overwrite audit ids by role and stream config
                if (InlongConstants.DATASYNC_REALTIME_MODE.equals(groupEntity.getInlongGroupMode())
                        || InlongConstants.DATASYNC_OFFLINE_MODE.equals(groupEntity.getInlongGroupMode())) {
                    List<AuditInformation> cdcAuditInfoList =
                            getCdcAuditInfoList(sourceNodeType, IndicatorType.RECEIVED_SUCCESS);
                    List<String> cdcAuditIdList =
                            cdcAuditInfoList.stream().map(v -> String.valueOf(v.getAuditId()))
                                    .collect(Collectors.toList());
                    if (CollectionUtils.isNotEmpty(cdcAuditIdList)) {
                        String tempSourceNodeType = sourceNodeType;
                        cdcAuditIdList.forEach(v -> auditIdMap.put(v, tempSourceNodeType));
                    }
                    auditIdMap.put(getAuditId(sourceNodeType, IndicatorType.RECEIVED_SUCCESS), sourceNodeType);
                    request.setAuditIds(getAuditIds(groupId, streamId, sourceNodeType, sinkNodeType));
                } else {
                    auditIdMap.put(getAuditId(sinkNodeType, IndicatorType.RECEIVED_SUCCESS), sinkNodeType);
                    request.setAuditIds(getAuditIds(groupId, streamId, null, sinkNodeType));
                }
            }
        } else if (CollectionUtils.isEmpty(request.getAuditIds())) {
            throw new BusinessException("audits id is empty");
        }

        List<AuditVO> result = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(request.getAuditIds().size());
        for (String auditId : request.getAuditIds()) {
            String auditName = auditItemMap.get(auditId);
            this.executor.execute(new AuditRunnable(request, auditId, auditName, result, latch, restTemplate,
                    auditQueryUrl, auditIdMap, false));
        }
        latch.await(30, TimeUnit.SECONDS);
        LOGGER.info("success to query audit list for request={}", request);
        return result;
    }

    @Override
    public List<AuditVO> listAll(AuditRequest request) throws Exception {
        List<AuditVO> result = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(request.getAuditIds().size());
        for (String auditId : request.getAuditIds()) {
            String auditName = auditItemMap.get(auditId);
            this.executor.execute(new AuditRunnable(request, auditId, auditName, result, latch, restTemplate,
                    auditQueryUrl, null, true));
        }
        latch.await(30, TimeUnit.SECONDS);
        return result;
    }

    @Override
    public List<AuditInformation> getAuditBases(Boolean isMetric) {
        if (isMetric) {
            return AuditOperator.getInstance().getAllMetricInformation();
        }
        return AuditOperator.getInstance().getAllAuditInformation();
    }

    private List<String> getAuditIds(String groupId, String streamId, String sourceNodeType, String sinkNodeType) {
        Set<String> auditSet = LoginUserUtils.getLoginUser().getRoles().contains(UserRoleCode.TENANT_ADMIN)
                ? new HashSet<>(auditIdListForAdmin)
                : new HashSet<>(auditIdListForUser);

        // if no sink is configured, return data-proxy output instead of sort
        if (sinkNodeType == null) {
            auditSet.add(getAuditId(ClusterType.DATAPROXY, IndicatorType.SEND_SUCCESS));
        } else {
            auditSet.add(getAuditId(sinkNodeType, IndicatorType.SEND_SUCCESS));
            InlongGroupEntity inlongGroup = inlongGroupMapper.selectByGroupId(groupId);
            if (InlongConstants.DATASYNC_REALTIME_MODE.equals(inlongGroup.getInlongGroupMode())
                    || InlongConstants.DATASYNC_OFFLINE_MODE.equals(inlongGroup.getInlongGroupMode())) {
                List<AuditInformation> cdcAuditInfoList =
                        getCdcAuditInfoList(sourceNodeType, IndicatorType.RECEIVED_SUCCESS);
                if (CollectionUtils.isNotEmpty(cdcAuditInfoList)) {
                    List<String> cdcAuditIdList =
                            cdcAuditInfoList.stream().map(v -> String.valueOf(v.getAuditId()))
                                    .collect(Collectors.toList());
                    auditSet.addAll(cdcAuditIdList);
                }
                auditSet.add(getAuditId(sourceNodeType, IndicatorType.RECEIVED_SUCCESS));
            } else {
                auditSet.add(getAuditId(sinkNodeType, IndicatorType.RECEIVED_SUCCESS));
            }
        }

        // auto push source has no agent, return data-proxy audit data instead of agent
        List<StreamSourceEntity> sourceList = sourceEntityMapper.selectByRelatedId(groupId, streamId, null);
        if (CollectionUtils.isEmpty(sourceList)
                || sourceList.stream().allMatch(s -> SourceType.AUTO_PUSH.equals(s.getSourceType()))) {
            // need data_proxy received type when agent has received type
            boolean dpReceivedNeeded = auditSet.contains(getAuditId(ClusterType.AGENT, IndicatorType.RECEIVED_SUCCESS));
            if (dpReceivedNeeded) {
                auditSet.add(getAuditId(ClusterType.DATAPROXY, IndicatorType.RECEIVED_SUCCESS));
            }
        }

        return new ArrayList<>(auditSet);
    }

    @Override
    public List<AuditProxy> getAuditProxy(String component) throws Exception {
        try {
            StringBuilder builder = new StringBuilder();
            builder.append(auditQueryUrl)
                    .append("/audit/query/getAuditProxy?")
                    .append("component=")
                    .append(component);
            String url = builder.toString();
            LOGGER.info("query audit url ={}", url);
            AuditProxyResponse result = HttpUtils.request(restTemplate,
                    url,
                    HttpMethod.GET, null,
                    null,
                    AuditProxyResponse.class);
            LOGGER.info("success to query audit proxy url list for request url ={}", url);
            return result.getData();
        } catch (Exception e) {
            String errMsg = String.format("get audit proxy url failed for %s", component);
            LOGGER.info(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }

    @Override
    public List<AuditInformation> getCdcAuditInfoList(String type, IndicatorType indicatorType) {
        if (StringUtils.isBlank(type)) {
            return null;
        }

        FlowType flowType = indicatorType.getCode() % 2 == 0 ? FlowType.INPUT : FlowType.OUTPUT;
        List<AuditInformation> cdcAuditInfo = AuditOperator.getInstance().getAllCdcIdInformation(type, flowType);
        Preconditions.expectNotNull(cdcAuditInfo, ErrorCodeEnum.AUDIT_ID_TYPE_NOT_SUPPORTED,
                String.format(ErrorCodeEnum.AUDIT_ID_TYPE_NOT_SUPPORTED.getMessage(), type));
        return cdcAuditInfo;
    }
}