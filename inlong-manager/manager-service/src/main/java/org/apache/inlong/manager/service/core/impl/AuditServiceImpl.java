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

import org.apache.commons.collections4.CollectionUtils;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.inlong.common.enums.IndicatorType.RECEIVED_SUCCESS;

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
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    // defaults to return all audit ids, can be overwritten in properties file
    // see audit id definitions: https://inlong.apache.org/docs/modules/audit/overview#audit-id
    @Value("#{'${audit.admin.ids:3,4,5,6}'.split(',')}")
    private List<String> auditIdListForAdmin;
    @Value("#{'${audit.user.ids:3,4,5,6}'.split(',')}")
    private List<String> auditIdListForUser;
    @Value("${audit.query.url:http://127.0.0.1:10080}")
    private String auditQueryUrl;
    @Value("${audit.query.queryTimeoutSeconds:30}")
    private int queryTimeoutSeconds;

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
        String sinkNodeType = request.getSinkType();
        // if sinkNodeType is not blank, should use directly
        if (StringUtils.isBlank(sinkNodeType) && Boolean.TRUE.equals(request.getNeedSinkAudit())) {
            sinkNodeType = getSinkNodeType(request.getSinkId(), groupId, streamId);
        }

        // key: auditId, value: nodeType
        Map<String, String> auditId2NodeTypeMap = new HashMap<>(8);
        fillAuditId2SinkNodeTypeMap(auditId2NodeTypeMap, sinkNodeType);

        // set sourceNodeType is sinkNodeType firstly
        String sourceNodeType = request.getSourceType();
        if (StringUtils.isBlank(sourceNodeType)) {
            sourceNodeType = sinkNodeType;
        }

        if (StringUtils.isNotBlank(groupId)) {
            List<StreamSourceEntity> sourceList = null;
            if (StringUtils.isNotBlank(streamId)) {
                // if sourceNodeType is blank, get sourceNodeType by groupId and streamId from StreamSourceEntity
                if (StringUtils.isBlank(sourceNodeType) && Boolean.TRUE.equals(request.getNeedSourceAudit())) {
                    sourceList = sourceEntityMapper.selectByRelatedId(groupId, streamId, null);
                    if (CollectionUtils.isNotEmpty(sourceList)) {
                        sourceNodeType = sourceList.get(0).getSourceType();
                        fillAuditId2SourceNodeTypeMap(auditId2NodeTypeMap, sourceNodeType);
                    }
                }
            }

            if (CollectionUtils.isEmpty(request.getAuditIds())) {
                InlongGroupEntity groupEntity = inlongGroupMapper.selectByGroupId(groupId);
                request.setAuditIds(getAuditIds(groupEntity.getInlongGroupMode(), sourceNodeType, sinkNodeType,
                        auditId2NodeTypeMap, sourceList));
            }
        } else if (CollectionUtils.isEmpty(request.getAuditIds())) {
            // groupId and auditIds cannot both be empty
            throw new BusinessException("InlongGroupId is blank, auditIds cannot be empty");
        }

        ConcurrentLinkedQueue<AuditVO> auditResultQueue = new ConcurrentLinkedQueue<>();
        CountDownLatch latch = new CountDownLatch(request.getAuditIds().size());
        for (String auditId : request.getAuditIds()) {
            String auditName = auditItemMap.get(auditId);
            this.executor.execute(new AuditRunnable(request, auditId, auditName, auditResultQueue, latch,
                    restTemplate, auditQueryUrl, auditId2NodeTypeMap, false));
        }

        boolean completed = latch.await(queryTimeoutSeconds, TimeUnit.SECONDS);
        if (!completed) {
            LOGGER.warn("Timeout to list audit for request={}", request);
        } else {
            LOGGER.info("Success to list audit for request={}", request);
        }

        return new ArrayList<>(auditResultQueue);
    }

    private String getSinkNodeType(Integer sinkId, String groupId, String streamId) {
        StreamSinkEntity sinkEntity = null;
        if (sinkId != null && sinkId > 0) {
            sinkEntity = sinkEntityMapper.selectByPrimaryKey(sinkId);
        } else if (StringUtils.isNoneBlank(groupId, streamId)) {
            List<StreamSinkEntity> sinkEntityList = sinkEntityMapper.selectByRelatedId(groupId, streamId);
            if (CollectionUtils.isNotEmpty(sinkEntityList)) {
                sinkEntity = sinkEntityList.get(0);
            }
        }
        // if sink info is existed, get sink type for query audit info.
        String sinkNodeType = null;
        if (sinkEntity != null) {
            sinkNodeType = sinkEntity.getSinkType();
        }

        return sinkNodeType;
    }

    /**
     * Fill the audit ID to node type mapping as fully as possible, avoiding null nodeType values.
     *
     * @param sourceNodeType source node type
     */
    private void fillAuditId2SourceNodeTypeMap(Map<String, String> resultMap, String sourceNodeType) {
        if (StringUtils.isBlank(sourceNodeType)) {
            return;
        }

        String sourceReceivedSuccessId = getAuditId(sourceNodeType, RECEIVED_SUCCESS);
        if (StringUtils.isNotBlank(sourceReceivedSuccessId)) {
            resultMap.put(sourceReceivedSuccessId, sourceNodeType);
        }
    }

    /**
     * Fill the audit ID to node type mapping as fully as possible, avoiding null nodeType values.
     *
     * @param sinkNodeType sink node type
     */
    private void fillAuditId2SinkNodeTypeMap(Map<String, String> resultMap, String sinkNodeType) {
        if (StringUtils.isBlank(sinkNodeType)) {
            return;
        }

        // Prevent null sink audit ID to avoid potential NPE during iteration
        String sinkSendSuccessId = getAuditId(sinkNodeType, IndicatorType.SEND_SUCCESS);
        if (StringUtils.isNotBlank(sinkSendSuccessId)) {
            resultMap.put(sinkSendSuccessId, sinkNodeType);
        }

        String sinkReceivedSuccessId = getAuditId(sinkNodeType, RECEIVED_SUCCESS);
        if (StringUtils.isNotBlank(sinkReceivedSuccessId)) {
            resultMap.put(sinkReceivedSuccessId, sinkNodeType);
        }
    }

    @Override
    public List<AuditVO> listAll(AuditRequest request) throws Exception {
        ConcurrentLinkedQueue<AuditVO> auditResultQueue = new ConcurrentLinkedQueue<>();
        CountDownLatch latch = new CountDownLatch(request.getAuditIds().size());
        for (String auditId : request.getAuditIds()) {
            String auditName = auditItemMap.get(auditId);
            this.executor.execute(new AuditRunnable(request, auditId, auditName, auditResultQueue, latch, restTemplate,
                    auditQueryUrl, null, true));
        }

        boolean completed = latch.await(queryTimeoutSeconds, TimeUnit.SECONDS);
        if (!completed) {
            LOGGER.warn("Timeout to list all audit for request={}", request);
        } else {
            LOGGER.info("Success to list all audit for request={}", request);
        }

        return new ArrayList<>(auditResultQueue);
    }

    @Override
    public List<AuditInformation> getAuditBases(Boolean isMetric) {
        if (isMetric) {
            return AuditOperator.getInstance().getAllMetricInformation();
        }
        return AuditOperator.getInstance().getAllAuditInformation();
    }

    private List<String> getAuditIds(Integer groupMode, String sourceNodeType, String sinkNodeType,
            Map<String, String> auditId2NodeTypeMap, List<StreamSourceEntity> sourceEntityList) {
        // properly overwrite audit ids by role and stream config
        boolean isDataSyncMode = InlongConstants.DATASYNC_REALTIME_MODE.equals(groupMode)
                || InlongConstants.DATASYNC_OFFLINE_MODE.equals(groupMode);
        if (isDataSyncMode) {
            List<AuditInformation> cdcAuditInfoList = getCdcAuditInfoList(sourceNodeType, RECEIVED_SUCCESS);
            List<String> cdcAuditIds = null;
            if (CollectionUtils.isNotEmpty(cdcAuditInfoList)) {
                cdcAuditIds = cdcAuditInfoList.stream()
                        .map(v -> String.valueOf(v.getAuditId()))
                        .collect(Collectors.toList());
                cdcAuditIds.forEach(v -> auditId2NodeTypeMap.put(v, sourceNodeType));
            }

            return getAuditIds(true, cdcAuditIds, sourceNodeType, sinkNodeType, sourceEntityList);
        } else {
            return getAuditIds(false, null, null, sinkNodeType, sourceEntityList);
        }
    }

    private List<String> getAuditIds(boolean isDataSyncMode, List<String> cdcAuditIds,
            String sourceNodeType, String sinkNodeType, List<StreamSourceEntity> sourceList) {
        Set<String> auditIdSet = LoginUserUtils.getLoginUser().getRoles().contains(UserRoleCode.TENANT_ADMIN)
                ? new HashSet<>(auditIdListForAdmin)
                : new HashSet<>(auditIdListForUser);

        // if no sink is configured, return data-proxy output instead of sort
        if (sinkNodeType == null) {
            auditIdSet.add(getAuditId(ClusterType.DATAPROXY, IndicatorType.SEND_SUCCESS));
        } else {
            auditIdSet.add(getAuditId(sinkNodeType, IndicatorType.SEND_SUCCESS));
            if (isDataSyncMode) {
                // if sourceNodeType is not blank, add cdc audit ids into auditSet
                if (StringUtils.isNotBlank(sourceNodeType)) {
                    if (CollectionUtils.isNotEmpty(cdcAuditIds)) {
                        auditIdSet.addAll(cdcAuditIds);
                    }
                    // add source received audit id into auditSet
                    auditIdSet.add(getAuditId(sourceNodeType, RECEIVED_SUCCESS));
                }
            } else {
                auditIdSet.add(getAuditId(sinkNodeType, RECEIVED_SUCCESS));
            }
        }

        // auto push source has no agent, return data-proxy audit data instead of agent
        if (CollectionUtils.isEmpty(sourceList)
                || sourceList.stream().allMatch(s -> SourceType.AUTO_PUSH.equals(s.getSourceType()))) {
            // need data_proxy received type when agent has received type
            boolean dpReceivedNeeded = auditIdSet.contains(getAuditId(ClusterType.AGENT, RECEIVED_SUCCESS));
            if (dpReceivedNeeded) {
                auditIdSet.add(getAuditId(ClusterType.DATAPROXY, RECEIVED_SUCCESS));
            }
        }

        return new ArrayList<>(auditIdSet);
    }

    @Override
    public List<AuditProxy> getAuditProxy(String component) throws Exception {
        try {
            String url = auditQueryUrl
                    + "/audit/query/getAuditProxy?"
                    + "component=" + component;
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