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

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.constant.Constants;
import org.apache.inlong.common.db.CommandEntity;
import org.apache.inlong.common.enums.PullJobTypeEnum;
import org.apache.inlong.common.enums.TaskTypeEnum;
import org.apache.inlong.common.pojo.agent.CmdConfig;
import org.apache.inlong.common.pojo.agent.DataConfig;
import org.apache.inlong.common.pojo.agent.TaskRequest;
import org.apache.inlong.common.pojo.agent.TaskResult;
import org.apache.inlong.common.pojo.agent.TaskSnapshotRequest;
import org.apache.inlong.common.pojo.dataproxy.DataProxyTopicInfo;
import org.apache.inlong.common.pojo.dataproxy.MQClusterInfo;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.common.constant.MQType;
import org.apache.inlong.manager.common.consts.SourceType;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.SourceStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongClusterEntity;
import org.apache.inlong.manager.dao.entity.InlongClusterNodeEntity;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.DataSourceCmdConfigEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongClusterNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterPageRequest;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterDTO;
import org.apache.inlong.manager.pojo.source.file.FileSourceDTO;
import org.apache.inlong.manager.service.core.AgentService;
import org.apache.inlong.manager.service.source.SourceSnapshotOperator;
import org.elasticsearch.common.util.set.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Agent service layer implementation
 */
@Service
public class AgentServiceImpl implements AgentService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentServiceImpl.class);
    private static final int UNISSUED_STATUS = 2;
    private static final int ISSUED_STATUS = 3;
    private static final int MODULUS_100 = 100;
    private static final int TASK_FETCH_SIZE = 2;

    @Autowired
    private StreamSourceEntityMapper sourceMapper;
    @Autowired
    private SourceSnapshotOperator snapshotOperator;
    @Autowired
    private DataSourceCmdConfigEntityMapper sourceCmdConfigMapper;
    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongStreamEntityMapper streamMapper;
    @Autowired
    private InlongClusterEntityMapper clusterMapper;
    @Autowired
    private InlongClusterNodeEntityMapper clusterNodeMapper;

    @Override
    public Boolean reportSnapshot(TaskSnapshotRequest request) {
        return snapshotOperator.snapshot(request);
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
    public void report(TaskRequest request) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("begin to get agent task: {}", request);
        }
        if (request == null || StringUtils.isBlank(request.getAgentIp())) {
            throw new BusinessException("agent request or agent ip was empty, just return");
        }

        // Update task status, other tasks with status 20x will change to 30x in next request
        if (CollectionUtils.isEmpty(request.getCommandInfo())) {
            LOGGER.info("task result was empty in request: {}, just return", request);
            return;
        }
        for (CommandEntity command : request.getCommandInfo()) {
            updateTaskStatus(command);
        }
    }

    /**
     * Update task status by command.
     *
     * @param command command info.
     */
    private void updateTaskStatus(CommandEntity command) {
        Integer taskId = command.getTaskId();
        StreamSourceEntity current = sourceMapper.selectForAgentTask(taskId);
        if (current == null) {
            LOGGER.warn("stream source not found by id={}, just return", taskId);
            return;
        }

        if (!Objects.equals(command.getVersion(), current.getVersion())) {
            LOGGER.warn("task result version [{}] not equals to current [{}] for id [{}], skip update",
                    command.getVersion(), current.getVersion(), taskId);
            return;
        }

        int result = command.getCommandResult();
        int previousStatus = current.getStatus();
        int nextStatus = SourceStatus.SOURCE_NORMAL.getCode();

        if (Constants.RESULT_FAIL == result) {
            LOGGER.warn("task failed for id =[{}]", taskId);
            nextStatus = SourceStatus.SOURCE_FAILED.getCode();
        } else if (previousStatus / MODULUS_100 == ISSUED_STATUS) {
            // Change the status from 30x to normal / disable / frozen
            if (SourceStatus.TEMP_TO_NORMAL.contains(previousStatus)) {
                nextStatus = SourceStatus.SOURCE_NORMAL.getCode();
            } else if (SourceStatus.BEEN_ISSUED_DELETE.getCode() == previousStatus) {
                nextStatus = SourceStatus.SOURCE_DISABLE.getCode();
            } else if (SourceStatus.BEEN_ISSUED_FROZEN.getCode() == previousStatus) {
                nextStatus = SourceStatus.SOURCE_FROZEN.getCode();
            }
        }

        if (nextStatus != previousStatus) {
            sourceMapper.updateStatus(taskId, nextStatus, false);
            LOGGER.info("task result=[{}], update source status to [{}] for id [{}]", result, nextStatus, taskId);
        }
    }

    @Override
    @Transactional(rollbackFor = Throwable.class, isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
    public TaskResult getTaskResult(TaskRequest request) {
        if (StringUtils.isBlank(request.getClusterName()) || StringUtils.isBlank(request.getAgentIp())) {
            throw new BusinessException("agent request or agent ip was empty, just return");
        }

        preProcessNormalTasks(request);
        List<DataConfig> tasks = fetchQueuedTasks(request);

        // Query pending special commands
        List<CmdConfig> cmdConfigs = getAgentCmdConfigs(request);
        return TaskResult.builder().dataConfigs(tasks).cmdConfigs(cmdConfigs).build();
    }

    /**
     * Query the tasks that source is need to add or active.  这里变成预处理，然后后面再进行统一的处理
     *
     * @param request
     * @return
     */
    private void preProcessNormalTasks(TaskRequest request) {
        // Query the tasks for non file collecting tasks
        preProcessNonFileTasks(request);
        // Query the tasks for file collecting tasks
        preProcessFileTasks(request);
    }

    /**
     * Query the tasks that source is waited to be operated.
     *
     * @param request
     * @return
     */
    private List<DataConfig> fetchQueuedTasks(TaskRequest request) {
        HashSet<SourceStatus> needAddStatusSet = Sets.newHashSet(SourceStatus.TOBE_ISSUED_SET);
        if (PullJobTypeEnum.NEVER == PullJobTypeEnum.getPullJobType(request.getPullJobType())) {
            LOGGER.warn("agent pull job type is [NEVER], just pull to be active tasks");
            needAddStatusSet.remove(SourceStatus.TO_BE_ISSUED_ADD);
        }

        // todo:弄清楚这里为什么一定需要uuid,如果是废的就不管它了
        List<StreamSourceEntity> sourceEntities = sourceMapper.selectByStatusAndCluster(
                needAddStatusSet.stream().map(SourceStatus::getCode).collect(Collectors.toList()),
                request.getClusterName(), request.getAgentIp(), request.getUuid());
        List<DataConfig> issuedTasks = Lists.newArrayList();
        for (StreamSourceEntity sourceEntity : sourceEntities) {
            int op = getOp(sourceEntity.getStatus());
            int nextStatus = getNextStatus(sourceEntity.getStatus());
            sourceEntity.setPreviousStatus(sourceEntity.getStatus());
            sourceEntity.setStatus(nextStatus);
            if (sourceMapper.updateByPrimaryKeySelective(sourceEntity) == 1) {
                sourceEntity.setVersion(sourceEntity.getVersion() + 1);
                issuedTasks.add(getDataConfig(sourceEntity, op));
            }
        }
        return issuedTasks;
    }

    private void preProcessNonFileTasks(TaskRequest taskRequest) {
        List<Integer> needAddStatusList;
        if (PullJobTypeEnum.NEVER == PullJobTypeEnum.getPullJobType(taskRequest.getPullJobType())) {
            LOGGER.warn("agent pull job type is [NEVER], just pull to be active tasks");
            needAddStatusList = Collections.singletonList(SourceStatus.TO_BE_ISSUED_ACTIVE.getCode());
        } else {
            needAddStatusList = Arrays.asList(SourceStatus.TO_BE_ISSUED_ADD.getCode(),
                    SourceStatus.TO_BE_ISSUED_ACTIVE.getCode());
        }
        List<String> sourceTypes = Lists.newArrayList(SourceType.MYSQL_SQL, SourceType.KAFKA,
                SourceType.MYSQL_BINLOG);
        List<StreamSourceEntity> sourceEntities = sourceMapper.selectByStatusAndType(needAddStatusList, sourceTypes,
                TASK_FETCH_SIZE);
        List<DataConfig> nonFileTasks = Lists.newArrayList();
        for (StreamSourceEntity sourceEntity : sourceEntities) {
            int op = getOp(sourceEntity.getStatus());
            int nextStatus = getNextStatus(sourceEntity.getStatus());
            sourceEntity.setStatus(nextStatus);
            sourceEntity.setAgentIp(taskRequest.getAgentIp());
            sourceEntity.setUuid(taskRequest.getUuid());
            if (sourceMapper.updateByPrimaryKeySelective(sourceEntity) == 1) {
                sourceEntity.setVersion(sourceEntity.getVersion() + 1);
                nonFileTasks.add(getDataConfig(sourceEntity, op));
            }
        }
    }

    private void preProcessFileTasks(TaskRequest taskRequest) {
        List<Integer> needAddStatusList;
        if (PullJobTypeEnum.NEVER == PullJobTypeEnum.getPullJobType(taskRequest.getPullJobType())) {
            LOGGER.warn("agent pull job type is [NEVER], just pull to be active tasks");
            needAddStatusList = Collections.singletonList(SourceStatus.TO_BE_ISSUED_ACTIVE.getCode());
        } else {
            needAddStatusList = Arrays.asList(SourceStatus.TO_BE_ISSUED_ADD.getCode(),
                    SourceStatus.TO_BE_ISSUED_ACTIVE.getCode());
        }
        final String agentIp = taskRequest.getAgentIp();
        final String agentClusterName = taskRequest.getClusterName();
        Preconditions.checkTrue(StringUtils.isNotBlank(agentIp) || StringUtils.isNotBlank(agentClusterName),
                "both agent ip and cluster name are blank when fetching file task");
        // find those node whose tag match stream_source tag and agent ip match stream_source agent ip
        InlongClusterNodeEntity clusterNodeEntity = selectByIpAndCluster(agentClusterName, agentIp);
        List<StreamSourceEntity> sourceEntities = sourceMapper.selectByAgentIpOrCluster(needAddStatusList,
                Lists.newArrayList(SourceType.FILE), agentIp, agentClusterName);
        sourceEntities.stream()
                .forEach(sourceEntity -> fetchFileTaskMatched(taskRequest, sourceEntity, clusterNodeEntity));

    }

    private InlongClusterNodeEntity selectByIpAndCluster(String clusterName, String ip) {
        InlongClusterEntity clusterEntity = clusterMapper.selectByNameAndType(clusterName, ClusterType.AGENT);
        if (clusterEntity == null) {
            return null;
        }

        ClusterPageRequest nodeRequest = new ClusterPageRequest();
        nodeRequest.setKeyword(ip);
        nodeRequest.setParentId(clusterEntity.getId());
        nodeRequest.setType(ClusterType.AGENT);
        InlongClusterNodeEntity clusterNodeEntity =
                clusterNodeMapper.selectByCondition(nodeRequest).stream().findFirst().orElse(null);
        return clusterNodeEntity;
    }

    /**
     * Find file collecting task match those condition:
     * 1.agent ip match
     * 2.node tag match
     * 3.cluster name match
     *
     * @param taskRequest
     * @param sourceEntity
     * @param clusterNodeEntity
     * @return
     */
    private void fetchFileTaskMatched(
            TaskRequest taskRequest, StreamSourceEntity sourceEntity, InlongClusterNodeEntity clusterNodeEntity) {
        // 先预处理，调整其状态；然后再处理，根据to_be状态进行处理
        // preprocessNonTemplateTask
        final String agentIp = taskRequest.getAgentIp();
        // fetch task by cluster name and template source
        boolean isTemplateTask = sourceEntity.getTemplateId() == null
                && StringUtils.isNotBlank(sourceEntity.getInlongClusterName())
                && sourceEntity.getInlongClusterName().equals(taskRequest.getClusterName());
        if (!isTemplateTask) {
            return;
        }

        // is the task already fetched by this agent ?
        List<StreamSourceEntity> subSources = sourceMapper.selectByTemplateId(sourceEntity.getId());
        Optional<StreamSourceEntity> optionalSource = subSources.stream()
                .filter(subSource -> subSource.getAgentIp().equals(agentIp))
                .findAny();
        if (optionalSource.isPresent()) {  // 有的情况下，可能有删除tag，以及删除tag后又增加tag的情况
            StreamSourceEntity fileEntity = optionalSource.get();
            // 如果不是最终态，则一直下发删除直到成为转变成为SOURCE_DISABLE状态，处理tag删除的逻辑
            if (!matchTag(optionalSource.get(), clusterNodeEntity) && SourceStatus.SOURCE_DISABLE.getCode() != fileEntity.getStatus()) {
                sourceMapper.updateStatus(fileEntity.getId(), SourceStatus.TO_BE_ISSUED_DELETE.getCode(), false);
            }

            // 如果不是运行态，则一直下发增加直到成功转变成SOURCE_NORMAL状态，处理tag新增的逻辑
            if (matchTag(optionalSource.get(), clusterNodeEntity) && SourceStatus.SOURCE_NORMAL.getCode() != fileEntity.getStatus()) {
                sourceMapper.updateStatus(fileEntity.getId(), SourceStatus.TO_BE_ISSUED_ADD.getCode(), false);
            }
        } else if (!optionalSource.isPresent() && matchTag(sourceEntity, clusterNodeEntity)) {  // 没有的情况下只有在tag匹配情况下才下发任务
            // if not, clone a subtask for this Agent.
            // note: a new source name with random suffix is generated to adhere to the unique constraint
            StreamSourceEntity fileEntity = CommonBeanUtils.copyProperties(sourceEntity, StreamSourceEntity::new);
            fileEntity.setSourceName(fileEntity.getSourceName() + "-"
                    + RandomStringUtils.randomAlphanumeric(10).toLowerCase(Locale.ROOT));
            fileEntity.setTemplateId(sourceEntity.getId());
            // create new sub source task
            sourceMapper.insert(fileEntity);
        }
    }

    private int getOp(int status) {
        return status % MODULUS_100;
    }

    private int getNextStatus(int status) {
        int op = status % MODULUS_100;
        return ISSUED_STATUS * MODULUS_100 + op;
    }

    /**
     * Get the DataConfig from the stream source entity.
     *
     * @param entity stream source entity.
     * @param op operation code for add, delete, etc.
     * @return data config.
     */
    private DataConfig getDataConfig(StreamSourceEntity entity, int op) {
        DataConfig dataConfig = new DataConfig();
        dataConfig.setIp(entity.getAgentIp());
        dataConfig.setUuid(entity.getUuid());
        dataConfig.setOp(String.valueOf(op));
        dataConfig.setTaskId(entity.getId());
        dataConfig.setTaskType(getTaskType(entity));
        dataConfig.setTaskName(entity.getSourceName());
        dataConfig.setSnapshot(entity.getSnapshot());
        dataConfig.setVersion(entity.getVersion());

        String groupId = entity.getInlongGroupId();
        String streamId = entity.getInlongStreamId();
        dataConfig.setInlongGroupId(groupId);
        dataConfig.setInlongStreamId(streamId);

        InlongGroupEntity groupEntity = groupMapper.selectByGroupId(groupId);
        if (groupEntity == null) {
            throw new BusinessException(String.format("inlong group not found for groupId=%s", groupId));
        }
        InlongStreamEntity streamEntity = streamMapper.selectByIdentifier(groupId, streamId);
        if (streamEntity == null) {
            throw new BusinessException(
                    String.format("inlong stream not found for groupId=%s streamId=%s", groupId, streamId));
        }

        String extParams = entity.getExtParams();
        dataConfig.setSyncSend(streamEntity.getSyncSend());
        if (SourceType.FILE.equalsIgnoreCase(streamEntity.getDataType())) {
            String dataSeparator = streamEntity.getDataSeparator();
            extParams = (null != dataSeparator ? getExtParams(extParams, dataSeparator) : extParams);
        }
        dataConfig.setExtParams(extParams);

        int dataReportType = groupEntity.getDataReportType();
        dataConfig.setDataReportType(dataReportType);
        if (InlongConstants.REPORT_TO_MQ_RECEIVED == dataReportType) {
            // add mq cluster setting
            List<MQClusterInfo> mqSet = new ArrayList<>();
            List<String> clusterTagList = Arrays.asList(groupEntity.getInlongClusterTag());
            List<String> typeList = Arrays.asList(ClusterType.TUBEMQ, ClusterType.PULSAR);
            ClusterPageRequest pageRequest = ClusterPageRequest.builder()
                    .typeList(typeList)
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
            InlongClusterEntity cluster = mqClusterList.get(0);
            String mqResource = groupEntity.getMqResource();
            String mqType = groupEntity.getMqType();
            if (MQType.PULSAR.equals(mqType) || MQType.TDMQ_PULSAR.equals(mqType)) {
                PulsarClusterDTO pulsarCluster = PulsarClusterDTO.getFromJson(cluster.getExtParams());
                String tenant = pulsarCluster.getTenant();
                if (StringUtils.isBlank(tenant)) {
                    tenant = InlongConstants.DEFAULT_PULSAR_TENANT;
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
            }
        }
        return dataConfig;
    }

    private String getExtParams(String extParams, String dataSeparator) {
        FileSourceDTO fileSourceDTO = JsonUtils.parseObject(extParams, FileSourceDTO.class);
        if (Objects.nonNull(fileSourceDTO)) {
            fileSourceDTO.setDataSeparator(dataSeparator);
            return JsonUtils.toJsonString(fileSourceDTO);
        }
        return extParams;
    }

    /**
     * Get the Task type from the stream source entity.
     *
     * @param sourceEntity stream source info.
     * @return task type
     */
    private int getTaskType(StreamSourceEntity sourceEntity) {
        TaskTypeEnum taskType = SourceType.SOURCE_TASK_MAP.get(sourceEntity.getSourceType());
        if (taskType == null) {
            throw new BusinessException("Unsupported task type for source type " + sourceEntity.getSourceType());
        }
        return taskType.getType();
    }

    // todo:删除
    /**
     * Get the agent command config by the agent ip.
     *
     * @param taskRequest task request info.
     * @return agent command config list.
     */
    private List<CmdConfig> getAgentCmdConfigs(TaskRequest taskRequest) {
        return sourceCmdConfigMapper.queryCmdByAgentIp(taskRequest.getAgentIp()).stream().map(cmd -> {
            CmdConfig cmdConfig = new CmdConfig();
            cmdConfig.setDataTime(cmd.getSpecifiedDataTime());
            cmdConfig.setOp(cmd.getCmdType());
            cmdConfig.setId(cmd.getId());
            cmdConfig.setTaskId(cmd.getTaskId());
            return cmdConfig;
        }).collect(Collectors.toList());
    }

    private boolean matchTag(StreamSourceEntity sourceEntity, InlongClusterNodeEntity clusterNodeEntity) {
        Preconditions.checkNotNull(sourceEntity, "cluster must be valid");
        if (sourceEntity.getInlongClusterNodeTag() == null) {
            return true;
        }
        if (clusterNodeEntity == null) {
            return false;
        }

        Set<String> tags = Stream.of(
                clusterNodeEntity.getNodeTags().split(InlongConstants.COMMA)).collect(Collectors.toSet());
        return tags.contains(sourceEntity.getInlongClusterNodeTag());
    }

}
