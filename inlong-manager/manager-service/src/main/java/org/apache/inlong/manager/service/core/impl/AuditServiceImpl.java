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

import org.apache.inlong.common.enums.IndicatorType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.AuditQuerySource;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.TimeStaticsDim;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.AuditBaseEntity;
import org.apache.inlong.manager.dao.entity.AuditSourceEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.AuditBaseEntityMapper;
import org.apache.inlong.manager.dao.mapper.AuditEntityMapper;
import org.apache.inlong.manager.dao.mapper.AuditSourceEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.pojo.audit.AuditBaseResponse;
import org.apache.inlong.manager.pojo.audit.AuditInfo;
import org.apache.inlong.manager.pojo.audit.AuditRequest;
import org.apache.inlong.manager.pojo.audit.AuditSourceRequest;
import org.apache.inlong.manager.pojo.audit.AuditSourceResponse;
import org.apache.inlong.manager.pojo.audit.AuditVO;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserRoleCode;
import org.apache.inlong.manager.service.audit.AuditRunnable;
import org.apache.inlong.manager.service.audit.InlongAuditSourceOperator;
import org.apache.inlong.manager.service.audit.InlongAuditSourceOperatorFactory;
import org.apache.inlong.manager.service.core.AuditService;
import org.apache.inlong.manager.service.resource.sink.ck.ClickHouseConfig;
import org.apache.inlong.manager.service.resource.sink.es.ElasticsearchApi;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.jdbc.SQL;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Audit service layer implementation
 */
@Lazy
@Service
public class AuditServiceImpl implements AuditService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AuditServiceImpl.class);
    private static final Gson GSON = new GsonBuilder().create();
    private static final String SECOND_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String HOUR_FORMAT = "yyyy-MM-dd HH";
    private static final String DAY_FORMAT = "yyyy-MM-dd";
    private static final DateTimeFormatter SECOND_DATE_FORMATTER = DateTimeFormat.forPattern(SECOND_FORMAT);
    private static final DateTimeFormatter HOUR_DATE_FORMATTER = DateTimeFormat.forPattern(HOUR_FORMAT);
    private static final DateTimeFormatter DAY_DATE_FORMATTER = DateTimeFormat.forPattern(DAY_FORMAT);

    private static final double DEFAULT_BOOST = 1.0;
    private static final boolean ADJUST_PURE_NEGATIVE = true;
    private static final int QUERY_FROM = 0;
    private static final int QUERY_SIZE = 0;
    private static final String SORT_ORDER = "ASC";
    private static final String TERM_FILED = "log_ts";
    private static final String AGGREGATIONS_COUNT = "count";
    private static final String AGGREGATIONS_DELAY = "delay";
    private static final String AGGREGATIONS = "aggregations";
    private static final String BUCKETS = "buckets";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String INLONG_GROUP_ID = "inlong_group_id";
    private static final String INLONG_STREAM_ID = "inlong_stream_id";
    private static final String COUNT = "count";
    private static final String DELAY = "delay";
    private static final String TERMS = "terms";

    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    // key 1: type of audit, like pulsar, hive, key 2: indicator type, value : entity of audit base item
    private final Map<String, Map<Integer, AuditBaseEntity>> auditIndicatorMap = new ConcurrentHashMap<>();

    private final Map<String, AuditBaseEntity> auditItemMap = new ConcurrentHashMap<>();

    // defaults to return all audit ids, can be overwritten in properties file
    // see audit id definitions: https://inlong.apache.org/docs/modules/audit/overview#audit-id
    @Value("#{'${audit.admin.ids:3,4,5,6}'.split(',')}")
    private List<String> auditIdListForAdmin;
    @Value("#{'${audit.user.ids:3,4,5,6}'.split(',')}")
    private List<String> auditIdListForUser;

    @Value("${audit.query.source}")
    private String auditQuerySource;
    @Value("${audit.query.url:http://127.0.0.1:10080}")
    private String auditQueryUrl;

    @Autowired
    private AuditBaseEntityMapper auditBaseMapper;
    @Autowired
    private AuditEntityMapper auditEntityMapper;
    @Autowired
    private ElasticsearchApi elasticsearchApi;
    @Autowired
    private StreamSinkEntityMapper sinkEntityMapper;
    @Autowired
    private StreamSourceEntityMapper sourceEntityMapper;
    @Autowired
    private ClickHouseConfig config;
    @Autowired
    private AuditSourceEntityMapper auditSourceMapper;
    @Autowired
    private InlongGroupEntityMapper inlongGroupMapper;
    @Autowired
    private InlongAuditSourceOperatorFactory auditSourceOperatorFactory;
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
            List<AuditBaseEntity> auditBaseEntities = auditBaseMapper.selectAll();
            for (AuditBaseEntity auditBaseEntity : auditBaseEntities) {
                auditItemMap.put(auditBaseEntity.getAuditId(), auditBaseEntity);
                String type = auditBaseEntity.getType();
                Map<Integer, AuditBaseEntity> itemMap = auditIndicatorMap.computeIfAbsent(type, v -> new HashMap<>());
                itemMap.put(auditBaseEntity.getIndicatorType(), auditBaseEntity);
            }
        } catch (Throwable t) {
            LOGGER.error("failed to reload audit base item info", t);
            return false;
        }

        LOGGER.debug("success to reload audit base item info");
        return true;
    }

    @Override
    public Integer updateAuditSource(AuditSourceRequest request, String operator) {
        InlongAuditSourceOperator auditSourceOperator = auditSourceOperatorFactory.getInstance(request.getType());
        request.setUrl(auditSourceOperator.convertTo(request.getUrl()));

        String offlineUrl = request.getOfflineUrl();
        if (StringUtils.isNotBlank(offlineUrl)) {
            auditSourceMapper.offlineSourceByUrl(offlineUrl);
            LOGGER.info("success offline the audit source with url: {}", offlineUrl);
        }

        // TODO firstly we should check to see if it exists, updated if it exists, and created if it doesn't exist
        AuditSourceEntity entity = CommonBeanUtils.copyProperties(request, AuditSourceEntity::new);
        entity.setStatus(InlongConstants.DEFAULT_ENABLE_VALUE);
        entity.setCreator(operator);
        entity.setModifier(operator);
        auditSourceMapper.insert(entity);
        Integer id = entity.getId();
        LOGGER.info("success to insert audit source with id={}", id);

        // TODO we should select the config that needs to be updated according to the source type
        config.updateRuntimeConfig();
        LOGGER.info("success to update audit source with id={}", id);

        return id;
    }

    @Override
    public AuditSourceResponse getAuditSource() {
        AuditSourceEntity entity = auditSourceMapper.selectOnlineSource();
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.RECORD_NOT_FOUND);
        }

        LOGGER.debug("success to get audit source, id={}", entity.getId());
        return CommonBeanUtils.copyProperties(entity, AuditSourceResponse::new);
    }

    @Override
    public String getAuditId(String type, IndicatorType indicatorType) {
        if (StringUtils.isBlank(type)) {
            return null;
        }
        Map<Integer, AuditBaseEntity> itemMap = auditIndicatorMap.computeIfAbsent(type, v -> new HashMap<>());
        AuditBaseEntity auditBaseEntity = itemMap.get(indicatorType.getCode());
        if (auditBaseEntity != null) {
            return auditBaseEntity.getAuditId();
        }
        auditBaseEntity = auditBaseMapper.selectByTypeAndIndicatorType(type, indicatorType.getCode());
        Preconditions.expectNotNull(auditBaseEntity, ErrorCodeEnum.AUDIT_ID_TYPE_NOT_SUPPORTED,
                String.format(ErrorCodeEnum.AUDIT_ID_TYPE_NOT_SUPPORTED.getMessage(), type));
        itemMap.put(auditBaseEntity.getIndicatorType(), auditBaseEntity);
        return auditBaseEntity.getAuditId();
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
                if (InlongConstants.DATASYNC_MODE.equals(groupEntity.getInlongGroupMode())) {
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
        AuditQuerySource querySource = AuditQuerySource.valueOf(auditQuerySource);
        CountDownLatch latch = new CountDownLatch(request.getAuditIds().size());
        for (String auditId : request.getAuditIds()) {
            AuditBaseEntity auditBaseEntity = auditItemMap.get(auditId);
            String auditName = auditBaseEntity != null ? auditBaseEntity.getName() : "";

            if (AuditQuerySource.MYSQL == querySource) {
                String format = "%Y-%m-%d %H:%i:00";
                // Support min agg at now
                DateTime endDate = DAY_DATE_FORMATTER.parseDateTime(request.getEndDate());
                String endDateStr = endDate.plusDays(1).toString(DAY_DATE_FORMATTER);
                List<Map<String, Object>> sumList =
                        StringUtils.isNotBlank(request.getIp()) ? auditEntityMapper.sumByLogTsAndIp(request.getIp(),
                                auditId, request.getStartDate(), endDateStr, format)
                                : auditEntityMapper.sumByLogTs(groupId, streamId, auditId, request.getStartDate(),
                                        endDateStr, format);
                List<AuditInfo> auditSet = sumList.stream().map(s -> {
                    AuditInfo vo = new AuditInfo();
                    vo.setInlongGroupId((String) s.get("inlongGroupId"));
                    vo.setInlongStreamId((String) s.get("inlongStreamId"));
                    vo.setLogTs((String) s.get("logTs"));
                    vo.setCount(((BigDecimal) s.get("total")).longValue());
                    vo.setDelay(((BigDecimal) s.get("totalDelay")).longValue());
                    vo.setSize(((BigDecimal) s.get("totalSize")).longValue());
                    return vo;
                }).collect(Collectors.toList());
                result.add(new AuditVO(auditId, auditName, auditSet, auditIdMap.getOrDefault(auditId, null)));
            } else if (AuditQuerySource.CLICKHOUSE == querySource) {
                this.executor.execute(new AuditRunnable(request, auditId, auditName, result, latch, restTemplate,
                        auditQueryUrl, auditIdMap, false));
            }
        }
        if (AuditQuerySource.CLICKHOUSE == querySource) {
            latch.await(30, TimeUnit.SECONDS);
        } else {
            result = aggregateByTimeDim(result, request.getTimeStaticsDim());
        }
        LOGGER.info("success to query audit list for request={}", request);
        return result;
    }

    @Override
    public List<AuditVO> listAll(AuditRequest request) throws Exception {
        List<AuditVO> result = new ArrayList<>();
        AuditQuerySource querySource = AuditQuerySource.valueOf(auditQuerySource);
        CountDownLatch latch = new CountDownLatch(request.getAuditIds().size());
        for (String auditId : request.getAuditIds()) {
            AuditBaseEntity auditBaseEntity = auditItemMap.get(auditId);
            String auditName = "";
            if (auditBaseEntity != null) {
                auditName = auditBaseEntity.getName();
            }
            if (AuditQuerySource.MYSQL == querySource) {
                // Support min agg at now
                DateTime endDate = SECOND_DATE_FORMATTER.parseDateTime(request.getEndDate());
                String endDateStr = endDate.plusDays(1).toString(SECOND_DATE_FORMATTER);
                List<Map<String, Object>> sumList = auditEntityMapper.sumGroupByIp(request.getInlongGroupId(),
                        request.getInlongStreamId(), request.getIp(), auditId, request.getStartDate(), endDateStr);
                List<AuditInfo> auditSet = sumList.stream().map(s -> {
                    AuditInfo vo = new AuditInfo();
                    vo.setInlongGroupId((String) s.get("inlongGroupId"));
                    vo.setInlongStreamId((String) s.get("inlongStreamId"));
                    vo.setLogTs((String) s.get("logTs"));
                    vo.setIp((String) s.get("ip"));
                    vo.setCount(((BigDecimal) s.get("total")).longValue());
                    vo.setDelay(((BigDecimal) s.get("totalDelay")).longValue());
                    vo.setSize(((BigDecimal) s.get("totalSize")).longValue());
                    return vo;
                }).collect(Collectors.toList());
                result.add(new AuditVO(auditId, auditName, auditSet, null));
            } else if (AuditQuerySource.CLICKHOUSE == querySource) {
                this.executor.execute(new AuditRunnable(request, auditId, auditName, result, latch, restTemplate,
                        auditQueryUrl, null, true));
            }
        }
        if (AuditQuerySource.CLICKHOUSE == querySource) {
            latch.await(30, TimeUnit.SECONDS);
        }
        return result;
    }

    @Override
    public List<AuditBaseResponse> getAuditBases() {
        List<AuditBaseEntity> auditBaseEntityList = auditBaseMapper.selectAll();
        return CommonBeanUtils.copyListProperties(auditBaseEntityList, AuditBaseResponse::new);
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
            if (InlongConstants.DATASYNC_MODE.equals(inlongGroup.getInlongGroupMode())) {
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

    /**
     * Get clickhouse Statement
     *
     * @param groupId The groupId of inlong
     * @param streamId The streamId of inlong
     * @param auditId The auditId of request
     * @param startDate The start datetime of request
     * @param endDate The en datetime of request
     * @return The clickhouse Statement
     */
    private PreparedStatement getAuditCkStatementGroupByLogTs(Connection connection, String groupId, String streamId,
            String ip,
            String auditId, String startDate, String endDate) throws SQLException {
        String start = DAY_DATE_FORMATTER.parseDateTime(startDate).toString(SECOND_FORMAT);
        String end = DAY_DATE_FORMATTER.parseDateTime(endDate).plusDays(1).toString(SECOND_FORMAT);
        if (StringUtils.isNotBlank(ip)) {
            return getAuditCkStatementByIp(connection, auditId, ip, startDate, endDate);
        }
        // Query results are duplicated according to all fields.
        String subQuery = new SQL()
                .SELECT_DISTINCT("ip", "docker_id", "thread_id", "sdk_ts", "packet_id", "log_ts", "inlong_group_id",
                        "inlong_stream_id", "audit_id", "count", "size", "delay")
                .FROM("audit_data")
                .WHERE("inlong_group_id = ?")
                .WHERE("inlong_stream_id = ?")
                .WHERE("audit_id = ?")
                .WHERE("log_ts >= ?")
                .WHERE("log_ts < ?")
                .toString();

        String sql = new SQL()
                .SELECT("inlong_group_id", "inlong_stream_id", "log_ts", "sum(count) as total",
                        "sum(delay) as total_delay", "sum(size) as total_size")
                .FROM("(" + subQuery + ") as sub")
                .GROUP_BY("log_ts", "inlong_group_id", "inlong_stream_id")
                .ORDER_BY("log_ts")
                .toString();

        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, groupId);
        statement.setString(2, streamId);
        statement.setString(3, auditId);
        statement.setString(4, start);
        statement.setString(5, end);
        return statement;
    }

    private PreparedStatement getAuditCkStatementGroupByIp(Connection connection, String groupId,
            String streamId, String ip, String auditId, String startDate, String endDate) throws SQLException {

        if (StringUtils.isNotBlank(ip)) {
            return getAuditCkStatementByIpGroupByIp(connection, auditId, ip, startDate, endDate);
        }
        // Query results are duplicated according to all fields.
        String subQuery = new SQL()
                .SELECT_DISTINCT("ip", "docker_id", "thread_id", "sdk_ts", "packet_id", "log_ts", "inlong_group_id",
                        "inlong_stream_id", "audit_id", "count", "size", "delay")
                .FROM("audit_data")
                .WHERE("inlong_group_id = ?")
                .WHERE("inlong_stream_id = ?")
                .WHERE("audit_id = ?")
                .WHERE("log_ts >= ?")
                .WHERE("log_ts < ?")
                .toString();

        String sql = new SQL()
                .SELECT("inlong_group_id", "inlong_stream_id", "sum(count) as total", "ip",
                        "sum(delay) as total_delay", "sum(size) as total_size")
                .FROM("(" + subQuery + ") as sub")
                .GROUP_BY("inlong_group_id", "inlong_stream_id", "ip")
                .toString();
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, groupId);
        statement.setString(2, streamId);
        statement.setString(3, auditId);
        statement.setString(4, startDate);
        statement.setString(5, endDate);
        return statement;
    }

    /**
     * Aggregate by time dim
     */
    private List<AuditVO> aggregateByTimeDim(List<AuditVO> auditVOList, TimeStaticsDim timeStaticsDim) {
        List<AuditVO> result;
        switch (timeStaticsDim) {
            case HOUR:
                result = doAggregate(auditVOList, HOUR_FORMAT);
                break;
            case DAY:
                result = doAggregate(auditVOList, DAY_FORMAT);
                break;
            default:
                result = doAggregate(auditVOList, SECOND_FORMAT);
                break;
        }
        return result;
    }

    /**
     * Execute the aggregate by the given time format
     */
    private List<AuditVO> doAggregate(List<AuditVO> auditVOList, String format) {
        List<AuditVO> result = new ArrayList<>();
        for (AuditVO auditVO : auditVOList) {
            AuditVO statInfo = new AuditVO();
            HashMap<String, AtomicLong> countMap = new HashMap<>();
            HashMap<String, AtomicLong> delayMap = new HashMap<>();
            HashMap<String, AtomicLong> sizeMap = new HashMap<>();
            statInfo.setAuditId(auditVO.getAuditId());
            statInfo.setAuditName(auditVO.getAuditName());
            statInfo.setNodeType(auditVO.getNodeType());
            for (AuditInfo auditInfo : auditVO.getAuditSet()) {
                String statKey = formatLogTime(auditInfo.getLogTs(), format);
                if (statKey == null) {
                    continue;
                }
                countMap.computeIfAbsent(statKey, k -> new AtomicLong(0)).addAndGet(auditInfo.getCount());
                delayMap.computeIfAbsent(statKey, k -> new AtomicLong(0)).addAndGet(auditInfo.getDelay());
                sizeMap.computeIfAbsent(statKey, k -> new AtomicLong(0)).addAndGet(auditInfo.getSize());
            }

            List<AuditInfo> auditInfoList = new LinkedList<>();
            for (Map.Entry<String, AtomicLong> entry : countMap.entrySet()) {
                AuditInfo auditInfoStat = new AuditInfo();
                auditInfoStat.setLogTs(entry.getKey());
                long count = entry.getValue().get();
                auditInfoStat.setCount(entry.getValue().get());
                auditInfoStat.setDelay(count == 0 ? 0 : delayMap.get(entry.getKey()).get() / count);
                auditInfoStat.setSize(count == 0 ? 0 : sizeMap.get(entry.getKey()).get() / count);
                auditInfoList.add(auditInfoStat);
            }
            statInfo.setAuditSet(auditInfoList);
            result.add(statInfo);
        }
        return result;
    }

    /**
     * Format the log time
     */
    private String formatLogTime(String dateString, String format) {
        String formatDateString = null;
        try {
            SimpleDateFormat formatter = new SimpleDateFormat(format);
            Date date = formatter.parse(dateString);
            formatDateString = formatter.format(date);
        } catch (Exception e) {
            LOGGER.error("format lot time exception", e);
        }
        return formatDateString;
    }

    private PreparedStatement getAuditCkStatementByIp(Connection connection, String auditId, String ip,
            String startDate, String endDate) throws SQLException {
        String start = DAY_DATE_FORMATTER.parseDateTime(startDate).toString(SECOND_FORMAT);
        String end = DAY_DATE_FORMATTER.parseDateTime(endDate).plusDays(1).toString(SECOND_FORMAT);
        String subQuery = new SQL()
                .SELECT_DISTINCT("ip", "docker_id", "thread_id", "sdk_ts", "packet_id", "log_ts", "inlong_group_id",
                        "inlong_stream_id", "audit_id", "count", "size", "delay")
                .FROM("audit_data")
                .WHERE("ip = ?")
                .WHERE("audit_id = ?")
                .WHERE("log_ts >= ?")
                .WHERE("log_ts < ?")
                .toString();

        String sql = new SQL()
                .SELECT("inlong_group_id", "inlong_stream_id", "log_ts", "sum(count) as total",
                        "sum(delay) as total_delay", "sum(size) as total_size")
                .FROM("(" + subQuery + ") as sub")
                .GROUP_BY("log_ts", "inlong_group_id", "inlong_stream_id")
                .ORDER_BY("log_ts")
                .toString();

        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, ip);
        statement.setString(2, auditId);
        statement.setString(3, start);
        statement.setString(4, end);
        return statement;
    }

    private PreparedStatement getAuditCkStatementByIpGroupByIp(Connection connection, String auditId, String ip,
            String startDate, String endDate) throws SQLException {
        String subQuery = new SQL()
                .SELECT_DISTINCT("ip", "docker_id", "thread_id", "sdk_ts", "packet_id", "log_ts", "inlong_group_id",
                        "inlong_stream_id", "audit_id", "count", "size", "delay")
                .FROM("audit_data")
                .WHERE("ip = ?")
                .WHERE("audit_id = ?")
                .WHERE("log_ts >= ?")
                .WHERE("log_ts < ?")
                .toString();

        String sql = new SQL()
                .SELECT("inlong_group_id", "inlong_stream_id", "ip", "sum(count) as total",
                        "sum(delay) as total_delay", "sum(size) as total_size")
                .FROM("(" + subQuery + ") as sub")
                .GROUP_BY("inlong_group_id", "inlong_stream_id", "ip")
                .toString();
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, ip);
        statement.setString(2, auditId);
        statement.setString(3, startDate);
        statement.setString(4, endDate);
        return statement;
    }

}
