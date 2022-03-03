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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.common.pojo.agent.CmdConfig;
import org.apache.inlong.common.pojo.agent.DataConfig;
import org.apache.inlong.common.pojo.agent.TaskRequest;
import org.apache.inlong.common.pojo.agent.TaskResult;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.enums.FileAgentDataGenerateRule;
import org.apache.inlong.manager.common.enums.SourceType;
import org.apache.inlong.manager.common.pojo.agent.AgentStatusReportRequest;
import org.apache.inlong.manager.common.pojo.agent.CheckAgentTaskConfRequest;
import org.apache.inlong.manager.common.pojo.agent.ConfirmAgentIpRequest;
import org.apache.inlong.manager.common.pojo.agent.FileAgentCMDConfig;
import org.apache.inlong.manager.common.pojo.agent.FileAgentCommandInfo;
import org.apache.inlong.manager.common.pojo.agent.FileAgentCommandInfo.CommandInfoBean;
import org.apache.inlong.manager.common.pojo.agent.FileAgentTaskConfig;
import org.apache.inlong.manager.common.pojo.agent.FileAgentTaskInfo;
import org.apache.inlong.manager.dao.entity.DataSourceCmdConfigEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.InlongStreamFieldEntity;
import org.apache.inlong.manager.dao.entity.SourceFileDetailEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.DataSourceCmdConfigEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamFieldEntityMapper;
import org.apache.inlong.manager.dao.mapper.SourceFileDetailEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.apache.inlong.manager.service.core.AgentTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class AgentTaskServiceImpl implements AgentTaskService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AgentTaskServiceImpl.class);

    @Autowired
    private SourceFileDetailEntityMapper fileDetailMapper;

    @Autowired
    private DataSourceCmdConfigEntityMapper sourceCmdConfigMapper;

    @Autowired
    private StreamSourceEntityMapper streamSourceMapper;

    @Autowired
    private InlongStreamEntityMapper inlongStreamMapper;

    @Autowired
    private InlongStreamFieldEntityMapper streamFieldMapper;

    @Override
    public TaskResult getAgentTask(TaskRequest taskRequest) {
        LOGGER.debug("begin to get agent task by taskRequestDto={}", taskRequest);
        if (taskRequest == null || taskRequest.getAgentIp() == null) {
            LOGGER.error("agent command taskRequestDto cannot be empty");
            return null;
        }
        // Query pending tasks by agentIp
        List<DataConfig> dataConfigs = getAgentDataConfigs(taskRequest);

        // Query pending special commands
        List<CmdConfig> cmdConfigs = getAgentCmdConfigs(taskRequest);

        return TaskResult.builder().dataConfigs(dataConfigs).cmdConfigs(cmdConfigs).build();
    }

    private List<DataConfig> getAgentDataConfigs(TaskRequest taskRequest) {
        List<DataConfig> dataConfigs = Lists.newArrayList();
        List<StreamSourceEntity> sourceEntities = streamSourceMapper.selectAgentTaskDataConfig(taskRequest);
        for (StreamSourceEntity sourceEntity : sourceEntities) {
            DataConfig dataConfig = new DataConfig();
            dataConfig.setOp(String.valueOf(sourceEntity.getStatus() % 100));
            dataConfig.setJobId(sourceEntity.getId());
            SourceType sourceType = SourceType.forType(sourceEntity.getSourceType());
            dataConfig.setTaskType(sourceType.getTaskType().getType());
            String inlongGroupId = sourceEntity.getInlongGroupId();
            String inlongStreamId = sourceEntity.getInlongStreamId();
            dataConfig.setInlongGroupId(inlongGroupId);
            dataConfig.setInlongStreamId(inlongStreamId);
            dataConfig.setIp(sourceEntity.getAgentIp());
            dataConfig.setUuid(sourceEntity.getUuid());
            dataConfig.setExtParams(sourceEntity.getExtParams());
            dataConfig.setSnapshot(sourceEntity.getSnapshot());
            InlongStreamEntity inlongStreamEntity = inlongStreamMapper.selectByIdentifier(inlongGroupId,inlongStreamId);
            inlongStreamEntity.setSyncSend(inlongStreamEntity.getSyncSend());
            dataConfigs.add(dataConfig);
        }
        //Forward Compatible File task type
        return dataConfigs;
    }

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

    @Deprecated
    @Override
    public FileAgentTaskInfo getFileAgentTask(FileAgentCommandInfo info) {
        LOGGER.debug("begin to get file agent task by info={}", info);
        if (info == null || info.getAgentIp() == null) {
            LOGGER.error("agent command info cannot be empty");
            return null;
        }

        // WorkflowProcess the status of the postback task
        this.dealCommandResult(info);

        // Query pending tasks
        List<FileAgentTaskConfig> taskConfigs = this.getFileAgentTaskConfigs(info);

        // Query pending special commands
        List<FileAgentCMDConfig> cmdConfigs = getFileAgentCMDConfigs(info);

        return FileAgentTaskInfo.builder().cmdConfigs(cmdConfigs).dataConfigs(taskConfigs).build();
    }

    @Deprecated
    private List<FileAgentCMDConfig> getFileAgentCMDConfigs(FileAgentCommandInfo info) {
        return sourceCmdConfigMapper.queryCmdByAgentIp(info.getAgentIp()).stream().map(cmd -> {
            FileAgentCMDConfig cmdConfig = new FileAgentCMDConfig();
            cmdConfig.setDataTime(cmd.getSpecifiedDataTime());
            cmdConfig.setOp(cmd.getCmdType());
            cmdConfig.setId(cmd.getId());
            cmdConfig.setTaskId(cmd.getTaskId());
            return cmdConfig;
        }).collect(Collectors.toList());
    }

    @Deprecated
    private List<FileAgentTaskConfig> getFileAgentTaskConfigs(FileAgentCommandInfo info) {
        // Query pending special commands
        List<FileAgentTaskConfig> taskConfigs = fileDetailMapper.selectFileAgentTaskByIp(info.getAgentIp());
        for (FileAgentTaskConfig config : taskConfigs) {
            FileAgentDataGenerateRule ruleEnu = FileAgentDataGenerateRule.fromRuleValue(config.getDataGenerateRule());
            if (ruleEnu != null) {
                config.setScheduleTime(ruleEnu.getScheduleRule());
            }
            StringBuilder s = new StringBuilder();

            s.append("m=").append(config.getSortType()).append("&");
            s.append("iname=").append(config.getInlongStreamId()).append("&");
            if (config.getDataGenerateRule().equalsIgnoreCase("minute")) {
                s.append("p=t").append("&");
            }

            List<InlongStreamFieldEntity> preFields = streamFieldMapper
                    .selectStreamFields(config.getInlongGroupId(), config.getInlongStreamId());

            if (!config.getSortType().equalsIgnoreCase("13")) {
                int fIndex = 0;
                for (InlongStreamFieldEntity f : preFields) {
                    s.append("__addcol" + fIndex + "__" + f.getFieldName());
                    s.append("=");
                    s.append(f.getFieldValue());
                    s.append("&");
                }
            }

            config.setAdditionalAttr(s.substring(0, s.toString().length() - 1));
        }
        return taskConfigs;
    }

    @Deprecated
    private void dealCommandResult(FileAgentCommandInfo info) {
        if (CollectionUtils.isEmpty(info.getCommandInfo())) {
            LOGGER.warn("command info is empty, just return");
            return;
        }

        for (CommandInfoBean command : info.getCommandInfo()) {
            SourceFileDetailEntity current = fileDetailMapper.selectByPrimaryKey(command.getTaskId());
            if (current == null) {
                continue;
            }

            int op = command.getOp();
            if (op == 2 || op == 6 || op == 8) { // Channel results issued by special orders
                DataSourceCmdConfigEntity cmd = new DataSourceCmdConfigEntity();
                if (command.getId() > 0) { // Modify the data result status of special commands
                    cmd.setId(command.getId());
                    cmd.setBsend(true);
                    cmd.setModifyTime(new Date());
                    cmd.setResultInfo(String.valueOf(command.getCommandResult()));
                    sourceCmdConfigMapper.updateByPrimaryKeySelective(cmd);
                }

            } else { // Modify the result status of the data collection task
                if (current.getModifyTime().getTime() - command.getDeliveryTime() > 1000 * 5) {
                    log.warn(" task id {} receive heartbeat time delay more than 5's, skip it!",
                            command.getTaskId());
                    continue;
                }

                int result = command.getCommandResult();
                int nextStatus = EntityStatus.AGENT_NORMAL.getCode();
                int previousStatus = current.getStatus();
                if (previousStatus / 100 == 2) { // Modify 30x -> 10x
                    if (result == 0) { // Processed successfully
                        if (previousStatus == EntityStatus.AGENT_ADD.getCode()) {
                            nextStatus = EntityStatus.AGENT_NORMAL.getCode();
                        } else if (previousStatus == EntityStatus.AGENT_DELETE.getCode()) {
                            nextStatus = EntityStatus.AGENT_DISABLE.getCode();
                        }
                    } else if (result == 1) { // Processing failed
                        nextStatus = EntityStatus.AGENT_FAILURE.getCode();
                    }

                    SourceFileDetailEntity update = new SourceFileDetailEntity();
                    update.setId(command.getTaskId());
                    update.setStatus(nextStatus);
                    update.setPreviousStatus(previousStatus);
                    fileDetailMapper.updateByPrimaryKeySelective(update);
                }
            }
        }
    }

    @Override
    public String confirmAgentIp(ConfirmAgentIpRequest request) {
        for (String ip : request.getIpList()) {
            List<FileAgentTaskConfig> taskConfigs = fileDetailMapper.selectFileAgentTaskByIp(ip);
            if (!taskConfigs.isEmpty()) {
                return taskConfigs.get(0).getIp();
            }
        }
        throw new IllegalArgumentException("Do not find any agent info with the ip's in db.");
    }

    @Override
    public List<FileAgentTaskConfig> checkAgentTaskConf(CheckAgentTaskConfRequest request) {
        List<FileAgentTaskConfig> taskConfigs = fileDetailMapper.selectFileAgentTaskByIpForCheck(request.getAgentIp());
        LOGGER.info(request.getAgentIp() + " taskConfigs = " + taskConfigs);
        List<FileAgentTaskConfig> toAdds = getToBeAdded(request.getTaskInfo(), taskConfigs);
        List<Integer> toRemoves = getToBeRemoved(request.getTaskInfo(), taskConfigs);
        List<FileAgentTaskConfig> commons = commons(request.getTaskInfo(), taskConfigs);

        List<FileAgentTaskConfig> result = new ArrayList<>(toAdds.size() + toRemoves.size());

        for (FileAgentTaskConfig fileAgentTaskConfig : toAdds) {
            // ADD(0)
            int currentStatus = fileAgentTaskConfig.getStatus();
            if (currentStatus == 100) {
                fileAgentTaskConfig.setOp("0");
                setFileAgentTaskConfigAttr(fileAgentTaskConfig);
                result.add(fileAgentTaskConfig);
            }
            // 20x, 30x ignore
            // 101, 103, 104 ignore
        }
        // There is no in the database, but the agent reports that it has it, then delete it
        for (Integer remove : toRemoves) {
            FileAgentTaskConfig config = new FileAgentTaskConfig();
            // DEL(1)
            config.setOp("1");
            config.setTaskId(remove);
            result.add(config);
        }
        for (FileAgentTaskConfig fileAgentTaskConfig : commons) {
            // 20x, 30x ignore
            int currentStatus = fileAgentTaskConfig.getStatus();
            if (currentStatus == 101) {
                fileAgentTaskConfig.setOp("1");
                setFileAgentTaskConfigAttr(fileAgentTaskConfig);
                result.add(fileAgentTaskConfig);
            } else if (currentStatus == 104) {
                fileAgentTaskConfig.setOp("4");
                setFileAgentTaskConfigAttr(fileAgentTaskConfig);
                result.add(fileAgentTaskConfig);
            } else {
                // 100, 103 print log
                LOGGER.warn("Agent {} report task id = {} with status {}, skip task id fileAgentTaskConfig = {} ",
                        request.getAgentIp(),
                        fileAgentTaskConfig.getTaskId(),
                        currentStatus,
                        fileAgentTaskConfig);
            }
        }
        return result;
    }

    private void setFileAgentTaskConfigAttr(FileAgentTaskConfig config) {
        FileAgentDataGenerateRule ruleEnu = FileAgentDataGenerateRule.fromRuleValue(config.getDataGenerateRule());
        if (ruleEnu != null) {
            config.setScheduleTime(ruleEnu.getScheduleRule());
        }
        StringBuilder s = new StringBuilder();

        s.append("m=").append(config.getSortType()).append("&");
        s.append("iname=").append(config.getInlongStreamId()).append("&");
        if (config.getDataGenerateRule().equalsIgnoreCase("minute")) {
            s.append("p=t").append("&");
        }

        List<InlongStreamFieldEntity> preFields = streamFieldMapper.selectStreamFields(
                config.getInlongGroupId(),
                config.getInlongStreamId());

        if (!config.getSortType().equalsIgnoreCase("13")) {
            int fIndex = 0;
            for (InlongStreamFieldEntity f : preFields) {
                s.append("__addcol" + fIndex + "__" + f.getFieldName());
                s.append("=");
                s.append(f.getFieldValue());
                s.append("&");
            }
        }

        config.setAdditionalAttr(s.substring(0, s.toString().length() - 1));
    }

    @Override
    public String reportAgentStatus(AgentStatusReportRequest request) {
        Gson gson = new GsonBuilder().create();
        // TODO
        LOGGER.info(gson.toJson(request));
        return "Success";
    }

    private List<FileAgentTaskConfig> getToBeAdded(List<Integer> taskInfo, List<FileAgentTaskConfig> taskConfigs) {
        Map<Integer, FileAgentTaskConfig> all = new HashMap<>();
        for (FileAgentTaskConfig config : taskConfigs) {
            all.put(config.getTaskId(), config);
        }
        for (Integer entry : taskInfo) {
            all.remove(entry);
        }
        return new ArrayList<>(all.values());
    }

    private List<Integer> getToBeRemoved(List<Integer> taskInfo, List<FileAgentTaskConfig> taskConfigs) {
        List<Integer> toRemove = new ArrayList<>();
        Map<Integer, FileAgentTaskConfig> all = new HashMap<>();
        for (FileAgentTaskConfig config : taskConfigs) {
            all.put(config.getTaskId(), config);
        }
        for (Integer entry : taskInfo) {
            if (!all.containsKey(entry)) {
                toRemove.add(entry);
            }
        }
        return toRemove;
    }

    private List<FileAgentTaskConfig> commons(List<Integer> taskInfo, List<FileAgentTaskConfig> taskConfigs) {
        Map<Integer, FileAgentTaskConfig> all = new HashMap<>();
        List<FileAgentTaskConfig> commons = new ArrayList<>();
        for (FileAgentTaskConfig config : taskConfigs) {
            all.put(config.getTaskId(), config);
        }
        for (Integer entry : taskInfo) {
            if (all.containsKey(entry)) {
                commons.add(all.get(entry));
            }
        }
        return commons;
    }

}
