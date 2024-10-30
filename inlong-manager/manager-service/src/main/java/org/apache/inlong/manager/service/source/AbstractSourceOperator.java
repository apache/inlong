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

package org.apache.inlong.manager.service.source;

import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.common.enums.TaskStateEnum;
import org.apache.inlong.common.enums.TaskTypeEnum;
import org.apache.inlong.common.pojo.agent.AgentConfigInfo;
import org.apache.inlong.common.pojo.agent.AgentResponseCode;
import org.apache.inlong.common.pojo.agent.CmdConfig;
import org.apache.inlong.common.pojo.agent.DataConfig;
import org.apache.inlong.common.pojo.agent.TaskResult;
import org.apache.inlong.common.pojo.dataproxy.DataProxyTopicInfo;
import org.apache.inlong.common.pojo.dataproxy.MQClusterInfo;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SourceStatus;
import org.apache.inlong.manager.common.enums.StreamStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.AgentTaskConfigEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamFieldEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceFieldEntity;
import org.apache.inlong.manager.dao.mapper.AgentTaskConfigEntityMapper;
import org.apache.inlong.manager.dao.mapper.DataSourceCmdConfigEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamFieldEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceFieldEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterPageRequest;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterDTO;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarDTO;
import org.apache.inlong.manager.pojo.source.DataAddTaskRequest;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.source.file.FileSourceDTO;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.StreamField;
import org.apache.inlong.manager.service.node.DataNodeService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.pagehelper.Page;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.inlong.manager.common.consts.InlongConstants.DOT;
import static org.apache.inlong.manager.pojo.stream.InlongStreamExtParam.unpackExtParams;

/**
 * Default operator of stream source.
 */
public abstract class AbstractSourceOperator implements StreamSourceOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSourceOperator.class);
    private static final Gson GSON = new Gson();
    private static final int MODULUS_100 = 100;

    @Autowired
    protected StreamSourceEntityMapper sourceMapper;
    @Autowired
    protected StreamSourceFieldEntityMapper sourceFieldMapper;
    @Autowired
    protected InlongStreamFieldEntityMapper streamFieldMapper;
    @Autowired
    protected DataNodeService dataNodeService;
    @Autowired
    private AgentTaskConfigEntityMapper agentTaskConfigEntityMapper;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private DataSourceCmdConfigEntityMapper sourceCmdConfigMapper;
    @Autowired
    private InlongStreamEntityMapper streamMapper;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private AutowireCapableBeanFactory autowireCapableBeanFactory;
    private SourceOperatorFactory operatorFactory;

    /**
     * Getting the source type.
     *
     * @return source type string.
     */
    protected abstract String getSourceType();

    /**
     * Setting the parameters of the latest entity.
     *
     * @param request source request
     * @param targetEntity entity object which will set the new parameters.
     */
    protected abstract void setTargetEntity(SourceRequest request, StreamSourceEntity targetEntity);

    @Override
    public String getExtParams(StreamSourceEntity sourceEntity) {
        return sourceEntity.getExtParams();
    }

    @Override
    @Transactional(rollbackFor = Throwable.class)
    public Integer saveOpt(SourceRequest request, Integer streamStatus, String operator) {
        StreamSourceEntity entity = CommonBeanUtils.copyProperties(request, StreamSourceEntity::new);
        if (SourceType.AUTO_PUSH.equals(request.getSourceType())) {
            // auto push task needs not be issued to agent
            entity.setStatus(SourceStatus.SOURCE_NORMAL.getCode());
        } else if (StreamStatus.forCode(streamStatus).equals(StreamStatus.CONFIG_SUCCESSFUL)) {
            entity.setStatus(SourceStatus.TO_BE_ISSUED_ADD.getCode());
        } else {
            entity.setStatus(SourceStatus.SOURCE_NEW.getCode());
        }
        entity.setCreator(operator);
        entity.setModifier(operator);

        // get the ext params
        setTargetEntity(request, entity);
        sourceMapper.insert(entity);
        saveFieldOpt(entity, request.getFieldList());
        if (request.getEnableSyncSchema()) {
            syncSourceFieldInfo(request, operator);
        }
        if (StreamStatus.forCode(streamStatus).equals(StreamStatus.CONFIG_SUCCESSFUL)) {
            updateAgentTaskConfig(request, operator);
        }
        return entity.getId();
    }

    @Override
    public List<StreamField> getSourceFields(Integer sourceId) {
        List<StreamSourceFieldEntity> sourceFieldEntities = sourceFieldMapper.selectBySourceId(sourceId);
        return CommonBeanUtils.copyListProperties(sourceFieldEntities, StreamField::new);
    }

    @Override
    public PageResult<? extends StreamSource> getPageInfo(Page<StreamSourceEntity> entityPage) {
        if (CollectionUtils.isEmpty(entityPage)) {
            return PageResult.empty();
        }
        return PageResult.fromPage(entityPage).map(this::getFromEntity);
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public void updateOpt(SourceRequest request, Integer streamStatus, Integer groupMode, String operator) {
        StreamSourceEntity entity = sourceMapper.selectByIdForUpdate(request.getId());
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_INFO_NOT_FOUND,
                    String.format("not found source record by id=%d", request.getId()));
        }
        if (SourceType.AUTO_PUSH.equals(entity.getSourceType())) {
            updateFieldOpt(entity, request.getFieldList());
            return;
        }
        boolean allowUpdate = InlongConstants.DATASYNC_REALTIME_MODE.equals(groupMode)
                || InlongConstants.DATASYNC_OFFLINE_MODE.equals(groupMode)
                || SourceStatus.ALLOWED_UPDATE.contains(entity.getStatus());
        if (!allowUpdate) {
            throw new BusinessException(ErrorCodeEnum.SOURCE_OPT_NOT_ALLOWED,
                    String.format(
                            "source=%s is not allowed to update, please wait until its changed to final status or stop / frozen / delete it firstly",
                            entity));
        }
        String errMsg = String.format("source has already updated with groupId=%s, streamId=%s, name=%s, curVersion=%s",
                request.getInlongGroupId(), request.getInlongStreamId(), request.getSourceName(), request.getVersion());
        if (!Objects.equals(entity.getVersion(), request.getVersion())) {
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED, errMsg);
        }

        // source type cannot be changed
        if (!Objects.equals(entity.getSourceType(), request.getSourceType())) {
            throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER,
                    String.format("source type=%s cannot change to %s", entity.getSourceType(),
                            request.getSourceType()));
        }

        String groupId = request.getInlongGroupId();
        String streamId = request.getInlongStreamId();
        String sourceName = request.getSourceName();
        List<StreamSourceEntity> sourceList = sourceMapper.selectByRelatedId(groupId, streamId, sourceName);
        for (StreamSourceEntity sourceEntity : sourceList) {
            Integer sourceId = sourceEntity.getId();
            if (!Objects.equals(sourceId, request.getId())) {
                throw new BusinessException(ErrorCodeEnum.SOURCE_ALREADY_EXISTS,
                        String.format("source name=%s already exists with the groupId=%s streamId=%s", sourceName,
                                groupId, streamId));
            }
        }

        // setting updated parameters of stream source entity.
        setTargetEntity(request, entity);
        entity.setModifier(operator);
        entity.setPreviousStatus(entity.getStatus());

        // re-issue task if necessary
        if (InlongConstants.STANDARD_MODE.equals(groupMode)) {
            SourceStatus sourceStatus = SourceStatus.forCode(entity.getStatus());
            Integer nextStatus = entity.getStatus();
            if (StreamStatus.forCode(streamStatus).equals(StreamStatus.CONFIG_SUCCESSFUL)) {
                nextStatus = SourceStatus.TO_BE_ISSUED_RETRY.getCode();
            } else {
                switch (SourceStatus.forCode(entity.getStatus())) {
                    case SOURCE_NORMAL:
                    case HEARTBEAT_TIMEOUT:
                        nextStatus = SourceStatus.TO_BE_ISSUED_RETRY.getCode();
                        break;
                    case SOURCE_FAILED:
                        nextStatus = SourceStatus.SOURCE_NEW.getCode();
                        break;
                    default:
                        // others leave it be
                        break;
                }
            }
            entity.setStatus(nextStatus);
        }

        int rowCount = sourceMapper.updateByPrimaryKeySelective(entity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED, errMsg);
        }
        updateFieldOpt(entity, request.getFieldList());
        LOGGER.debug("success to update source of type={}", request.getSourceType());
        if (StreamStatus.forCode(streamStatus).equals(StreamStatus.CONFIG_SUCCESSFUL)) {
            updateAgentTaskConfig(request, operator);
        }
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public void stopOpt(SourceRequest request, String operator) {
        StreamSourceEntity existEntity = sourceMapper.selectByIdForUpdate(request.getId());
        SourceStatus curState = SourceStatus.forCode(existEntity.getStatus());
        SourceStatus nextState = SourceStatus.TO_BE_ISSUED_STOP;
        if (curState == SourceStatus.SOURCE_STOP) {
            return;
        }
        StreamSourceEntity curEntity = CommonBeanUtils.copyProperties(request, StreamSourceEntity::new);
        curEntity.setPreviousStatus(curState.getCode());
        curEntity.setStatus(nextState.getCode());
        int rowCount = sourceMapper.updateByPrimaryKeySelective(curEntity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.error("source has already updated with groupId={}, streamId={}, name={}, curVersion={}",
                    curEntity.getInlongGroupId(), curEntity.getInlongStreamId(), curEntity.getSourceName(),
                    curEntity.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
        updateAgentTaskConfig(request, operator);
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public void restartOpt(SourceRequest request, String operator) {
        StreamSourceEntity existEntity = sourceMapper.selectByIdForUpdate(request.getId());
        SourceStatus curState = SourceStatus.forCode(existEntity.getStatus());
        SourceStatus nextState = SourceStatus.TO_BE_ISSUED_ACTIVE;
        StreamSourceEntity curEntity = CommonBeanUtils.copyProperties(request, StreamSourceEntity::new);
        curEntity.setPreviousStatus(curState.getCode());
        curEntity.setStatus(nextState.getCode());
        int rowCount = sourceMapper.updateByPrimaryKeySelective(curEntity);
        if (rowCount != InlongConstants.AFFECTED_ONE_ROW) {
            LOGGER.error("source has already updated with groupId={}, streamId={}, name={}, curVersion={}",
                    curEntity.getInlongGroupId(), curEntity.getInlongStreamId(), curEntity.getSourceName(),
                    curEntity.getVersion());
            throw new BusinessException(ErrorCodeEnum.CONFIG_EXPIRED);
        }
    }

    protected void updateFieldOpt(StreamSourceEntity entity, List<StreamField> fieldInfos) {
        Integer sourceId = entity.getId();
        if (CollectionUtils.isEmpty(fieldInfos)) {
            return;
        }

        // Stream source fields
        sourceFieldMapper.deleteAll(sourceId);
        this.saveFieldOpt(entity, fieldInfos);

        // InLong stream fields
        String groupId = entity.getInlongGroupId();
        String streamId = entity.getInlongStreamId();
        streamFieldMapper.deleteAllByIdentifier(groupId, streamId);
        saveStreamField(groupId, streamId, fieldInfos);

        LOGGER.debug("success to update source fields");
    }

    protected void saveStreamField(String groupId, String streamId, List<StreamField> infoList) {
        if (CollectionUtils.isEmpty(infoList)) {
            return;
        }
        infoList.forEach(streamField -> streamField.setId(null));
        List<InlongStreamFieldEntity> list = CommonBeanUtils.copyListProperties(infoList,
                InlongStreamFieldEntity::new);
        for (InlongStreamFieldEntity entity : list) {
            entity.setInlongGroupId(groupId);
            entity.setInlongStreamId(streamId);
            entity.setIsDeleted(InlongConstants.UN_DELETED);
        }
        streamFieldMapper.insertAll(list);
    }

    protected void saveFieldOpt(StreamSourceEntity entity, List<StreamField> fieldInfos) {
        LOGGER.debug("begin to save source fields={}", fieldInfos);
        if (CollectionUtils.isEmpty(fieldInfos)) {
            return;
        }

        int size = fieldInfos.size();
        List<StreamSourceFieldEntity> entityList = new ArrayList<>(size);
        String groupId = entity.getInlongGroupId();
        String streamId = entity.getInlongStreamId();
        String sourceType = entity.getSourceType();
        Integer sourceId = entity.getId();
        for (StreamField fieldInfo : fieldInfos) {
            StreamSourceFieldEntity fieldEntity = CommonBeanUtils.copyProperties(fieldInfo,
                    StreamSourceFieldEntity::new);
            if (StringUtils.isEmpty(fieldEntity.getFieldComment())) {
                fieldEntity.setFieldComment(fieldEntity.getFieldName());
            }
            fieldEntity.setInlongGroupId(groupId);
            fieldEntity.setInlongStreamId(streamId);
            fieldEntity.setSourceId(sourceId);
            fieldEntity.setSourceType(sourceType);
            fieldEntity.setIsDeleted(InlongConstants.UN_DELETED);
            entityList.add(fieldEntity);
        }

        sourceFieldMapper.insertAll(entityList);
        LOGGER.debug("success to save source fields");
    }

    /**
     * If the stream source can only use one data type, return the data type that has been set.
     *
     * @param streamSource stream source
     * @param streamDataType stream data type
     * @return serialization type
     */
    protected String getSerializationType(StreamSource streamSource, String streamDataType) {
        if (StringUtils.isNotBlank(streamSource.getSerializationType())) {
            return streamSource.getSerializationType();
        }

        return DataTypeEnum.forType(streamDataType).getType();
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public void syncSourceFieldInfo(SourceRequest request, String operator) {
        LOGGER.info("not support sync source field info for type ={}", request.getSourceType());
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.REPEATABLE_READ)
    public Integer addDataAddTask(DataAddTaskRequest request, String operator) {
        throw new BusinessException(String.format("not support data add task for type =%s", request.getSourceType()));
    }

    @Override
    public void updateAgentTaskConfig(SourceRequest request, String operator) {
        try {
            if (SourceType.AUTO_PUSH.equals(request.getSourceType())) {
                return;
            }
            final String clusterName = request.getInlongClusterName();
            final String ip = request.getAgentIp();
            final String uuid = request.getUuid();
            if (StringUtils.isBlank(clusterName) || StringUtils.isBlank(ip)) {
                LOGGER.warn("skip update agent task config where cluster name or ip is null for request={}", request);
                return;
            }
            AgentTaskConfigEntity existEntity = agentTaskConfigEntityMapper.selectByIdentifier(ip, clusterName);
            AgentTaskConfigEntity agentTaskConfigEntity = new AgentTaskConfigEntity();
            if (existEntity != null) {
                agentTaskConfigEntity = CommonBeanUtils.copyProperties(existEntity, AgentTaskConfigEntity::new, true);
            }

            LOGGER.debug("begin to get agent config info for {}", request);
            Set<String> tagSet = new HashSet<>(16);
            InlongClusterEntity agentClusterInfo = clusterMapper.selectByNameAndType(request.getInlongClusterName(),
                    ClusterType.AGENT);
            if (agentClusterInfo == null) {
                agentTaskConfigEntity.setIsDeleted(agentTaskConfigEntity.getId());
                agentTaskConfigEntityMapper.updateByIdSelective(agentTaskConfigEntity);
                return;
            }
            String clusterTag = agentClusterInfo.getClusterTags();
            AgentConfigInfo agentConfigInfo = AgentConfigInfo.builder()
                    .cluster(AgentConfigInfo.AgentClusterInfo.builder()
                            .parentId(agentClusterInfo.getId())
                            .clusterName(agentClusterInfo.getName())
                            .build())
                    .build();
            if (StringUtils.isNotBlank(clusterTag)) {
                tagSet.addAll(Arrays.asList(clusterTag.split(InlongConstants.COMMA)));
                List<String> clusterTagList = new ArrayList<>(tagSet);
                ClusterPageRequest pageRequest = ClusterPageRequest.builder()
                        .type(ClusterType.AGENT_ZK)
                        .clusterTagList(clusterTagList)
                        .build();
                List<InlongClusterEntity> agentZkCluster = clusterMapper.selectByCondition(pageRequest);
                if (CollectionUtils.isNotEmpty(agentZkCluster)) {
                    agentConfigInfo.setZkUrl(agentZkCluster.get(0).getUrl());
                }
            }

            String jsonStr = GSON.toJson(agentConfigInfo);
            String configMd5 = DigestUtils.md5Hex(jsonStr);
            agentConfigInfo.setMd5(configMd5);
            agentConfigInfo.setCode(AgentResponseCode.SUCCESS);
            agentTaskConfigEntity.setConfigParams(objectMapper.writeValueAsString(agentConfigInfo));

            List<StreamSourceEntity> normalSourceEntities = sourceMapper.selectByStatusAndCluster(
                    SourceStatus.NORMAL_STATUS_SET.stream().map(SourceStatus::getCode)
                            .collect(Collectors.toList()),
                    clusterName, ip, uuid);
            List<StreamSourceEntity> taskLists = new ArrayList<>(normalSourceEntities);
            List<StreamSourceEntity> stopSourceEntities = sourceMapper.selectByStatusAndCluster(
                    SourceStatus.STOP_STATUS_SET.stream().map(SourceStatus::getCode)
                            .collect(Collectors.toList()),
                    clusterName, ip, uuid);
            taskLists.addAll(stopSourceEntities);
            LOGGER.debug("success to add task : {}", taskLists.size());
            List<DataConfig> runningTaskConfig = Lists.newArrayList();
            List<CmdConfig> cmdConfigs = sourceCmdConfigMapper.queryCmdByAgentIp(request.getAgentIp()).stream()
                    .map(cmd -> {
                        CmdConfig cmdConfig = new CmdConfig();
                        cmdConfig.setDataTime(cmd.getSpecifiedDataTime());
                        cmdConfig.setOp(cmd.getCmdType());
                        cmdConfig.setId(cmd.getId());
                        cmdConfig.setTaskId(cmd.getTaskId());
                        return cmdConfig;
                    }).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(taskLists)) {
                agentTaskConfigEntity.setTaskParams("");
                agentTaskConfigEntityMapper.updateByIdSelective(agentTaskConfigEntity);
                return;
            }
            for (StreamSourceEntity sourceEntity : taskLists) {
                int op = sourceEntity.getStatus() % MODULUS_100;
                DataConfig dataConfig = getDataConfig(sourceEntity, op);
                runningTaskConfig.add(dataConfig);
            }
            TaskResult taskResult =
                    TaskResult.builder().dataConfigs(runningTaskConfig).cmdConfigs(cmdConfigs).build();
            String md5 = DigestUtils.md5Hex(GSON.toJson(taskResult));
            taskResult.setMd5(md5);
            taskResult.setCode(AgentResponseCode.SUCCESS);
            agentTaskConfigEntity.setAgentIp(request.getAgentIp());
            agentTaskConfigEntity.setClusterName(request.getInlongClusterName());
            agentTaskConfigEntity.setTaskParams(objectMapper.writeValueAsString(taskResult));

            agentClusterInfo.setModifier(operator);
            if (existEntity == null) {
                agentTaskConfigEntity.setCreator(operator);
                agentTaskConfigEntityMapper.insert(agentTaskConfigEntity);
            } else {
                agentTaskConfigEntityMapper.updateByIdSelective(agentTaskConfigEntity);
            }
            LOGGER.debug("success to update agent config info for: {}, result: {}", request, agentConfigInfo);
        } catch (Exception e) {
            String errMsg = String.format("update agent task config failed for groupId=%s, streamId=%s, ip=%s",
                    request.getInlongGroupId(), request.getInlongStreamId(), request.getAgentIp());
            LOGGER.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }

    private DataConfig getDataConfig(StreamSourceEntity entity, int op) {
        DataConfig dataConfig = new DataConfig();
        dataConfig.setIp(entity.getAgentIp());
        dataConfig.setUuid(entity.getUuid());
        dataConfig.setOp(String.valueOf(op));
        dataConfig.setTaskId(entity.getId());
        dataConfig.setTaskType(getTaskType(entity));
        dataConfig.setTaskName(entity.getSourceName());
        dataConfig.setSnapshot(entity.getSnapshot());
        dataConfig.setTimeZone(entity.getDataTimeZone());
        dataConfig.setVersion(entity.getVersion());

        String groupId = entity.getInlongGroupId();
        String streamId = entity.getInlongStreamId();
        dataConfig.setInlongGroupId(groupId);
        dataConfig.setInlongStreamId(streamId);

        InlongGroupEntity groupEntity = groupMapper.selectByGroupIdWithoutTenant(groupId);
        InlongStreamEntity streamEntity = streamMapper.selectByIdentifier(groupId, streamId);
        if (operatorFactory == null) {
            operatorFactory = new SourceOperatorFactory();
            autowireCapableBeanFactory.autowireBean(operatorFactory);
        }
        StreamSourceOperator sourceOperator = operatorFactory.getInstance(entity.getSourceType());
        String extParams = sourceOperator.getExtParams(entity);
        if (groupEntity != null && streamEntity != null) {
            dataConfig.setState(
                    SourceStatus.NORMAL_STATUS_SET.contains(SourceStatus.forCode(entity.getStatus()))
                            ? TaskStateEnum.RUNNING.getType()
                            : TaskStateEnum.FROZEN.getType());
            dataConfig.setSyncSend(streamEntity.getSyncSend());
            if (SourceType.FILE.equalsIgnoreCase(entity.getSourceType())) {
                String dataSeparator = String.valueOf((char) Integer.parseInt(streamEntity.getDataSeparator()));
                FileSourceDTO fileSourceDTO = JsonUtils.parseObject(extParams, FileSourceDTO.class);
                if (Objects.nonNull(fileSourceDTO)) {
                    fileSourceDTO.setDataSeparator(dataSeparator);
                    dataConfig.setAuditVersion(fileSourceDTO.getAuditVersion());
                    fileSourceDTO.setDataContentStyle(streamEntity.getDataType());
                    extParams = JsonUtils.toJsonString(fileSourceDTO);
                }
            }
            InlongStreamInfo streamInfo = CommonBeanUtils.copyProperties(streamEntity, InlongStreamInfo::new);
            // Processing extParams
            unpackExtParams(streamEntity.getExtParams(), streamInfo);
            dataConfig.setPredefinedFields(streamInfo.getPredefinedFields());

            int dataReportType = groupEntity.getDataReportType();
            dataConfig.setDataReportType(dataReportType);
            if (InlongConstants.REPORT_TO_MQ_RECEIVED == dataReportType) {
                // add mq cluster setting
                List<MQClusterInfo> mqSet = new ArrayList<>();
                List<String> clusterTagList = Collections.singletonList(groupEntity.getInlongClusterTag());
                ClusterPageRequest pageRequest = ClusterPageRequest.builder()
                        .type(groupEntity.getMqType())
                        .clusterTagList(clusterTagList)
                        .build();
                List<InlongClusterEntity> mqClusterList = clusterMapper.selectByCondition(pageRequest);
                for (InlongClusterEntity cluster : mqClusterList) {
                    MQClusterInfo clusterInfo = new MQClusterInfo();
                    clusterInfo.setUrl(cluster.getUrl());
                    clusterInfo.setToken(cluster.getToken());
                    clusterInfo.setMqType(cluster.getType());
                    clusterInfo.setParams(JsonUtils.parseObject(cluster.getExtParams(), HashMap.class));
                    mqSet.add(clusterInfo);
                }
                dataConfig.setMqClusters(mqSet);

                // add topic setting
                String mqResource = groupEntity.getMqResource();
                String mqType = groupEntity.getMqType();
                if (MQType.PULSAR.equals(mqType) || MQType.TDMQ_PULSAR.equals(mqType)) {
                    // first get the tenant from the InlongGroup, and then get it from the PulsarCluster.
                    InlongPulsarDTO pulsarDTO = InlongPulsarDTO.getFromJson(groupEntity.getExtParams());
                    String tenant = pulsarDTO.getPulsarTenant();
                    if (StringUtils.isBlank(tenant)) {
                        // If there are multiple Pulsar clusters, take the first one.
                        // Note that the tenants in multiple Pulsar clusters must be identical.
                        PulsarClusterDTO pulsarCluster = PulsarClusterDTO.getFromJson(
                                mqClusterList.get(0).getExtParams());
                        tenant = pulsarCluster.getPulsarTenant();
                    }

                    String topic = String.format(InlongConstants.PULSAR_TOPIC_FORMAT,
                            tenant, mqResource, streamEntity.getMqResource());
                    DataProxyTopicInfo topicConfig = new DataProxyTopicInfo();
                    topicConfig.setInlongGroupId(groupId + "/" + streamId);
                    topicConfig.setTopic(topic);
                    dataConfig.setTopicInfo(topicConfig);
                } else if (MQType.TUBEMQ.equals(mqType)) {
                    DataProxyTopicInfo topicConfig = new DataProxyTopicInfo();
                    topicConfig.setInlongGroupId(groupId);
                    topicConfig.setTopic(mqResource);
                    dataConfig.setTopicInfo(topicConfig);
                } else if (MQType.KAFKA.equals(mqType)) {
                    DataProxyTopicInfo topicConfig = new DataProxyTopicInfo();
                    topicConfig.setInlongGroupId(groupId);
                    topicConfig.setTopic(groupEntity.getMqResource() + DOT + streamEntity.getMqResource());
                    dataConfig.setTopicInfo(topicConfig);
                }
            } else {
                LOGGER.warn("set syncSend=[0] as the stream not exists for groupId={}, streamId={}", groupId, streamId);
            }
        }
        dataConfig.setExtParams(extParams);
        return dataConfig;
    }

    private int getTaskType(StreamSourceEntity sourceEntity) {
        TaskTypeEnum taskType = SourceType.SOURCE_TASK_MAP.get(sourceEntity.getSourceType());
        if (taskType == null) {
            throw new BusinessException("Unsupported task type for source type " + sourceEntity.getSourceType());
        }
        return taskType.getType();
    }
}
