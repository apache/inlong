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
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.HttpUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.AuditAlertRuleEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.AuditAlertRuleEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.pojo.audit.AuditAlertCondition;
import org.apache.inlong.manager.pojo.audit.AuditAlertRule;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleRequest;
import org.apache.inlong.manager.pojo.audit.AuditAlertRuleUpdateRequest;
import org.apache.inlong.manager.pojo.audit.AuditProxyResponse;
import org.apache.inlong.manager.pojo.audit.AuditRequest;
import org.apache.inlong.manager.pojo.audit.AuditVO;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserRoleCode;
import org.apache.inlong.manager.service.audit.AuditRunnable;
import org.apache.inlong.manager.service.core.AuditService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final ObjectMapper objectMapper = new ObjectMapper();

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
    private AuditAlertRuleEntityMapper alertRuleMapper;
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
        LOGGER.debug("begin to get audit id by type={} and indicatorType={}", type, indicatorType);
        if (StringUtils.isBlank(type)) {
            LOGGER.warn("type is blank when getting audit id");
            return null;
        }
        Map<Integer, AuditInformation> itemMap = auditIndicatorMap.computeIfAbsent(type, v -> new HashMap<>());
        AuditInformation auditInformation = itemMap.get(indicatorType.getCode());
        if (auditInformation != null) {
            String auditId = String.valueOf(auditInformation.getAuditId());
            LOGGER.debug("success to get audit id={} by type={} and indicatorType={}", auditId, type, indicatorType);
            return auditId;
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
        String auditId = String.valueOf(auditInformation.getAuditId());
        LOGGER.debug("success to build and get audit id={} by type={} and indicatorType={}", auditId, type,
                indicatorType);
        return auditId;
    }

    @Override
    public List<AuditVO> listByCondition(AuditRequest request) throws Exception {
        LOGGER.info("begin to query audit list by condition, request={}", request);
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
        LOGGER.info("success to query audit list by condition, request={}", request);
        return result;
    }

    @Override
    public List<AuditVO> listAll(AuditRequest request) throws Exception {
        LOGGER.info("begin to list all audit data, request={}", request);
        List<AuditVO> result = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(request.getAuditIds().size());
        for (String auditId : request.getAuditIds()) {
            String auditName = auditItemMap.get(auditId);
            this.executor.execute(new AuditRunnable(request, auditId, auditName, result, latch, restTemplate,
                    auditQueryUrl, null, true));
        }
        latch.await(30, TimeUnit.SECONDS);
        LOGGER.info("success to list all audit data, request={}", request);
        return result;
    }

    private List<String> getAuditIds(String groupId, String streamId, String sourceNodeType, String sinkNodeType) {
        LOGGER.debug("begin to get audit ids, groupId={}, streamId={}, sourceNodeType={}, sinkNodeType={}",
                groupId, streamId, sourceNodeType, sinkNodeType);
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

        List<String> result = new ArrayList<>(auditSet);
        LOGGER.debug("success to get audit ids, result size={}", result.size());
        return result;
    }

    @Override
    public List<AuditProxy> getAuditProxy(String component) throws Exception {
        LOGGER.info("begin to get audit proxy by component={}", component);
        try {
            StringBuilder builder = new StringBuilder();
            builder.append(auditQueryUrl)
                    .append("/audit/query/getAuditProxy?")
                    .append("component=")
                    .append(component);
            String url = builder.toString();
            LOGGER.debug("query audit url ={}", url);
            AuditProxyResponse result = HttpUtils.request(restTemplate,
                    url,
                    HttpMethod.GET, null,
                    null,
                    AuditProxyResponse.class);
            LOGGER.info("success to get audit proxy by component={}", component);
            return result.getData();
        } catch (Exception e) {
            String errMsg = String.format("get audit proxy url failed for %s", component);
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }

    @Override
    public List<AuditInformation> getAuditBases(Boolean isMetric) {
        LOGGER.debug("begin to list audit bases, isMetric={}", isMetric);
        List<AuditInformation> result;
        if (isMetric) {
            result = AuditOperator.getInstance().getAllMetricInformation();
        } else {
            result = AuditOperator.getInstance().getAllAuditInformation();
        }
        LOGGER.debug("success to list audit bases, isMetric={}, result size={}", isMetric, result.size());
        return result;
    }

    @Override
    public List<AuditInformation> getCdcAuditInfoList(String type, IndicatorType indicatorType) {
        LOGGER.debug("begin to list cdc audit info, type={}, indicatorType={}", type, indicatorType);
        if (StringUtils.isBlank(type)) {
            LOGGER.warn("type is blank when listing cdc audit info");
            return null;
        }

        FlowType flowType = indicatorType.getCode() % 2 == 0 ? FlowType.INPUT : FlowType.OUTPUT;
        List<AuditInformation> cdcAuditInfo = AuditOperator.getInstance().getAllCdcIdInformation(type, flowType);
        Preconditions.expectNotNull(cdcAuditInfo, ErrorCodeEnum.AUDIT_ID_TYPE_NOT_SUPPORTED,
                String.format(ErrorCodeEnum.AUDIT_ID_TYPE_NOT_SUPPORTED.getMessage(), type));
        LOGGER.debug("success to list cdc audit info, type={}, indicatorType={}, result size={}",
                type, indicatorType, cdcAuditInfo.size());
        return cdcAuditInfo;
    }

    @Override
    public List<AuditAlertRule> listRules(String inlongGroupId, String inlongStreamId) {
        LOGGER.info("begin to list audit alert rules, inlongGroupId={}, inlongStreamId={}", inlongGroupId,
                inlongStreamId);
        List<AuditAlertRuleEntity> entities = alertRuleMapper.selectByGroupAndStream(inlongGroupId, inlongStreamId);
        List<AuditAlertRule> result = entities.stream()
                .filter(entity -> entity.getIsDeleted() == null || entity.getIsDeleted() == 0)
                .map(entity -> {
                    AuditAlertRule rule = CommonBeanUtils.copyProperties(entity, AuditAlertRule::new);
                    // Convert condition JSON string to Condition object
                    if (StringUtils.isNotBlank(entity.getCondition())) {
                        try {
                            AuditAlertCondition condition =
                                    objectMapper.readValue(entity.getCondition(), AuditAlertCondition.class);
                            rule.setCondition(condition);
                        } catch (JsonProcessingException e) {
                            LOGGER.error("Failed to parse condition JSON: {}", entity.getCondition(), e);
                            throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Invalid condition format");
                        }
                    }
                    return rule;
                })
                .collect(Collectors.toList());
        LOGGER.info("success to list audit alert rules, inlongGroupId={}, inlongStreamId={}, result size={}",
                inlongGroupId, inlongStreamId, result.size());
        return result;
    }

    @Override
    public AuditAlertRule create(AuditAlertRule rule, String operator) {
        LOGGER.info("begin to create audit alert rule, rule={}, operator={}", rule, operator);

        // Validate input
        Preconditions.expectNotBlank(rule.getInlongGroupId(), ErrorCodeEnum.INVALID_PARAMETER,
                "inlongGroupId cannot be blank");
        Preconditions.expectNotBlank(rule.getAuditId(), ErrorCodeEnum.INVALID_PARAMETER, "auditId cannot be blank");
        Preconditions.expectNotBlank(rule.getAlertName(), ErrorCodeEnum.INVALID_PARAMETER, "alertName cannot be blank");
        Preconditions.expectNotNull(rule.getCondition(), ErrorCodeEnum.INVALID_PARAMETER, "condition cannot be null");

        // Convert to entity
        AuditAlertRuleEntity entity = CommonBeanUtils.copyProperties(rule, AuditAlertRuleEntity::new);
        entity.setCreator(operator);
        entity.setModifier(operator);
        // Set isDeleted to 0 by default
        if (entity.getIsDeleted() == null) {
            entity.setIsDeleted(0);
        }

        // Convert Condition object to JSON string for database storage
        if (rule.getCondition() != null) {
            try {
                String conditionJson = objectMapper.writeValueAsString(rule.getCondition());
                entity.setCondition(conditionJson);
            } catch (JsonProcessingException e) {
                LOGGER.error("Failed to serialize condition to JSON: {}", rule.getCondition(), e);
                throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Invalid condition format");
            }
        }

        // Set default values if needed
        if (entity.getEnabled() == null) {
            entity.setEnabled(true);
        }
        if (StringUtils.isBlank(entity.getLevel())) {
            entity.setLevel("WARN");
        }
        if (StringUtils.isBlank(entity.getNotifyType())) {
            entity.setNotifyType("EMAIL");
        }

        // Set default version to 1
        if (entity.getVersion() == null) {
            entity.setVersion(1);
        }

        // Insert into database
        int result = alertRuleMapper.insert(entity);
        if (result <= 0) {
            LOGGER.error("Failed to create audit alert rule, rule={}", rule);
            throw new BusinessException(ErrorCodeEnum.GROUP_SAVE_FAILED, "Failed to create audit alert rule");
        }

        // Convert back to POJO with Condition object
        AuditAlertRule resultRule = CommonBeanUtils.copyProperties(entity, AuditAlertRule::new);
        if (StringUtils.isNotBlank(entity.getCondition())) {
            try {
                AuditAlertCondition condition =
                        objectMapper.readValue(entity.getCondition(), AuditAlertCondition.class);
                resultRule.setCondition(condition);
            } catch (JsonProcessingException e) {
                LOGGER.error("Failed to parse condition JSON: {}", entity.getCondition(), e);
                throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Invalid condition format");
            }
        }
        LOGGER.info("success to create audit alert rule, rule={}, operator={}", rule, operator);
        return resultRule;
    }

    @Override
    public Integer create(AuditAlertRuleRequest request, String operator) {
        LOGGER.info("begin to create audit alert rule from request, request={}, operator={}", request, operator);

        // Validate input
        Preconditions.expectNotNull(request, ErrorCodeEnum.INVALID_PARAMETER, "request cannot be null");
        Preconditions.expectNotBlank(request.getInlongGroupId(), ErrorCodeEnum.INVALID_PARAMETER,
                "inlongGroupId cannot be blank");
        Preconditions.expectNotBlank(request.getAuditId(), ErrorCodeEnum.INVALID_PARAMETER, "auditId cannot be blank");
        Preconditions.expectNotBlank(request.getAlertName(), ErrorCodeEnum.INVALID_PARAMETER,
                "alertName cannot be blank");
        Preconditions.expectNotNull(request.getCondition(), ErrorCodeEnum.INVALID_PARAMETER,
                "condition cannot be null");

        // Convert request to rule
        AuditAlertRule rule = CommonBeanUtils.copyProperties(request, AuditAlertRule::new);

        // Set default values if needed
        if (rule.getEnabled() == null) {
            rule.setEnabled(true);
        }
        if (StringUtils.isBlank(rule.getLevel())) {
            rule.setLevel("WARN");
        }
        if (StringUtils.isBlank(rule.getNotifyType())) {
            rule.setNotifyType("EMAIL");
        }

        // Create the rule using existing method
        AuditAlertRule savedRule = create(rule, operator);
        LOGGER.info("success to create audit alert rule from request, request={}, operator={}, ruleId={}",
                request, operator, savedRule.getId());
        return savedRule.getId();
    }

    @Override
    public AuditAlertRule get(Integer id) {
        LOGGER.info("begin to get audit alert rule by id={}", id);

        Preconditions.expectNotNull(id, ErrorCodeEnum.INVALID_PARAMETER, "rule id cannot be null");

        AuditAlertRuleEntity entity = alertRuleMapper.selectById(id);
        if (entity == null) {
            LOGGER.warn("Audit alert rule not found with id: {}", id);
            throw new BusinessException(ErrorCodeEnum.RECORD_NOT_FOUND, "Audit alert rule not found with id: " + id);
        }
        // Exclude soft-deleted record
        if (entity.getIsDeleted() != null && entity.getIsDeleted() != 0) {
            LOGGER.warn("Audit alert rule is deleted with id: {}", id);
            throw new BusinessException(ErrorCodeEnum.RECORD_NOT_FOUND, "Audit alert rule not found with id: " + id);
        }

        AuditAlertRule rule = CommonBeanUtils.copyProperties(entity, AuditAlertRule::new);
        // Convert condition JSON string to Condition object
        if (StringUtils.isNotBlank(entity.getCondition())) {
            try {
                AuditAlertCondition condition =
                        objectMapper.readValue(entity.getCondition(), AuditAlertCondition.class);
                rule.setCondition(condition);
            } catch (JsonProcessingException e) {
                LOGGER.error("Failed to parse condition JSON: {}", entity.getCondition(), e);
                throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Invalid condition format");
            }
        }
        LOGGER.info("success to get audit alert rule by id={}", id);
        return rule;
    }

    @Override
    public AuditAlertRule update(AuditAlertRule rule, String operator) {
        LOGGER.info("begin to update audit alert rule, rule={}, operator={}", rule, operator);

        // Validate input
        Preconditions.expectNotNull(rule.getId(), ErrorCodeEnum.INVALID_PARAMETER, "rule id cannot be null");

        // Check if exists
        AuditAlertRuleEntity existingEntity = alertRuleMapper.selectById(rule.getId());
        if (existingEntity == null) {
            LOGGER.warn("Audit alert rule not found with id: {}", rule.getId());
            throw new BusinessException(ErrorCodeEnum.RECORD_NOT_FOUND,
                    "Audit alert rule not found with id: " + rule.getId());
        }

        LOGGER.debug("Existing entity version: {}, Incoming rule version: {}", existingEntity.getVersion(),
                rule.getVersion());

        // Version check for optimistic locking
        // The version passed in should be the current version in database
        // DAO layer will check version = #{version} - 1
        if (rule.getVersion() == null || !rule.getVersion().equals(existingEntity.getVersion())) {
            LOGGER.warn(
                    "Audit alert rule config has been modified, please refresh and try again. Existing version: {}, Incoming version: {}",
                    existingEntity.getVersion(), rule.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED,
                    "Audit alert rule config has been modified, please refresh and try again. Existing version: "
                            + existingEntity.getVersion() + ", Incoming version: " + rule.getVersion());
        }

        // Convert to entity and set modifier
        AuditAlertRuleEntity entity = CommonBeanUtils.copyProperties(rule, AuditAlertRuleEntity::new);
        entity.setModifier(operator);
        // Increment version
        entity.setVersion(existingEntity.getVersion() + 1);

        LOGGER.debug("Updating entity with version: {}", entity.getVersion());

        // Convert Condition object to JSON string for database storage
        if (rule.getCondition() != null) {
            try {
                String conditionJson = objectMapper.writeValueAsString(rule.getCondition());
                entity.setCondition(conditionJson);
            } catch (JsonProcessingException e) {
                LOGGER.error("Failed to serialize condition to JSON: {}", rule.getCondition(), e);
                throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Invalid condition format");
            }
        }

        // Update in database
        int result = alertRuleMapper.updateById(entity);
        LOGGER.debug("Update result: {} rows affected", result);
        if (result <= 0) {
            LOGGER.error("Failed to update audit alert rule, rule={}", rule);
            throw new BusinessException(ErrorCodeEnum.GROUP_SAVE_FAILED, "Failed to update audit alert rule");
        }

        // Return updated entity
        AuditAlertRuleEntity updatedEntity = alertRuleMapper.selectById(rule.getId());
        AuditAlertRule resultRule = CommonBeanUtils.copyProperties(updatedEntity, AuditAlertRule::new);
        // Convert condition JSON string to Condition object
        if (StringUtils.isNotBlank(updatedEntity.getCondition())) {
            try {
                AuditAlertCondition condition =
                        objectMapper.readValue(updatedEntity.getCondition(), AuditAlertCondition.class);
                resultRule.setCondition(condition);
            } catch (JsonProcessingException e) {
                LOGGER.error("Failed to parse condition JSON: {}", updatedEntity.getCondition(), e);
                throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Invalid condition format");
            }
        }
        LOGGER.info("success to update audit alert rule, rule={}, operator={}", rule, operator);
        return resultRule;
    }

    @Override
    public AuditAlertRule update(AuditAlertRuleUpdateRequest request, String operator) {
        LOGGER.info("begin to update audit alert rule from request, request={}, operator={}", request, operator);

        // Validate input
        Preconditions.expectNotNull(request, ErrorCodeEnum.INVALID_PARAMETER, "request cannot be null");
        Preconditions.expectNotNull(request.getId(), ErrorCodeEnum.INVALID_PARAMETER, "rule id cannot be null");

        // Convert request to rule
        AuditAlertRule rule = CommonBeanUtils.copyProperties(request, AuditAlertRule::new);

        // Update the rule using existing method
        AuditAlertRule result = update(rule, operator);
        LOGGER.info("success to update audit alert rule from request, request={}, operator={}", request, operator);
        return result;
    }

    @Override
    public Boolean delete(Integer id) {
        LOGGER.info("begin to delete audit alert rule by id={}", id);

        Preconditions.expectNotNull(id, ErrorCodeEnum.INVALID_PARAMETER, "rule id cannot be null");

        // Check if exists
        AuditAlertRuleEntity existingEntity = alertRuleMapper.selectById(id);
        if (existingEntity == null) {
            LOGGER.warn("Audit alert rule not found with id: {}", id);
            throw new BusinessException(ErrorCodeEnum.RECORD_NOT_FOUND, "Audit alert rule not found with id: " + id);
        }

        // Soft delete - set is_deleted to the record id instead of physical deletion
        existingEntity.setIsDeleted(id);
        // Increment version for optimistic locking
        existingEntity.setVersion(existingEntity.getVersion() + 1);
        int result = alertRuleMapper.updateById(existingEntity);
        if (result <= 0) {
            LOGGER.error("Failed to delete audit alert rule by id={}", id);
            throw new BusinessException(ErrorCodeEnum.GROUP_DELETE_NOT_ALLOWED, "Failed to delete audit alert rule");
        }

        LOGGER.info("success to delete audit alert rule by id={}", id);
        return true;
    }

    @Override
    public List<AuditAlertRule> listEnabled() {
        LOGGER.info("begin to list enabled audit alert rules");
        List<AuditAlertRule> allRules = listRules(null, null);
        List<AuditAlertRule> result = allRules.stream()
                .filter(rule -> (rule.getIsDeleted() == null || rule.getIsDeleted() == 0))
                .filter(AuditAlertRule::getEnabled)
                .collect(Collectors.toList());
        LOGGER.info("success to list enabled audit alert rules, result size={}", result.size());
        return result;
    }
}
