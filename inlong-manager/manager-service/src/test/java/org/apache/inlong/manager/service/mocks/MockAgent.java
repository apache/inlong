package org.apache.inlong.manager.service.mocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.common.constant.Constants;
import org.apache.inlong.common.db.CommandEntity;
import org.apache.inlong.common.enums.ComponentTypeEnum;
import org.apache.inlong.common.enums.PullJobTypeEnum;
import org.apache.inlong.common.heartbeat.HeartbeatMsg;
import org.apache.inlong.common.pojo.agent.TaskRequest;
import org.apache.inlong.common.pojo.agent.TaskResult;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.pojo.cluster.ClusterNodeBindTagRequest;
import org.apache.inlong.manager.pojo.heartbeat.HeartbeatReportRequest;
import org.apache.inlong.manager.service.core.AgentService;
import org.apache.inlong.manager.service.core.HeartbeatService;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.inlong.manager.service.ServiceBaseTest.GLOBAL_OPERATOR;

public class MockAgent {
    // mock ability
    // 1.定时向manager拉取任务，并处理该任务
    // 2.定时向manager上报之前执行的任务情况（可能成功可能失败）
    // validate ability
    // 1.某个任务的下发次数
    // 2.任务保存状态（是否真的在运行）

    
    public static final String LOCAL_IP = "127.0.0.1";
    public static final String LOCAL_PORT = "8008";
    public static final String LOCAL_TAG = "default_tag";
    public static final String CLUSTER_TAG = "default_cluster_tag";
    public static final String CLUSTER_NAME = "1c59ef9e8e1e43cfb25ee8b744c9c81b_2790";

    
    private AgentService agentService;
    private HeartbeatService heartbeatService;

    // 模拟本机的配置
    private Queue<CommandEntity> commands = new LinkedList<>();
    private Set<String> tags = Sets.newHashSet(LOCAL_TAG);
    private int jobLimit;

    public MockAgent(AgentService agentService, HeartbeatService heartbeatService, int jobLimit) {
        this.agentService = agentService;
        this.heartbeatService = heartbeatService;
        this.jobLimit = jobLimit;
    }

    public TaskResult pullTask() {
        TaskRequest request = new TaskRequest();
        request.setAgentIp(LOCAL_IP);
        request.setClusterName(CLUSTER_NAME);
        request.setPullJobType(PullJobTypeEnum.NEW.getType());
        request.setCommandInfo(new ArrayList<>());
        while (!commands.isEmpty()) {
            request.getCommandInfo().add(commands.poll());
        }
        agentService.report(request);
        TaskResult result = agentService.getTaskResult(request);
        mockHandleTask(result);
        return result;
    }

    public void sendHeartbeat() {
        HeartbeatReportRequest heartbeat = new HeartbeatReportRequest();
        heartbeat.setIp(LOCAL_IP);
        heartbeat.setPort(LOCAL_PORT);
        heartbeat.setComponentType(ComponentTypeEnum.Agent.getType());
        heartbeat.setClusterName(CLUSTER_NAME);
        heartbeat.setClusterTag(CLUSTER_TAG);
        heartbeat.setNodeTag(tags.stream().collect(Collectors.joining(InlongConstants.COMMA)));
        heartbeat.setInCharges(GLOBAL_OPERATOR);
        heartbeat.setReportTime(System.currentTimeMillis());
        heartbeatService.reportHeartbeat(heartbeat);
    }

    public void bindTag(boolean bind, String tag) {
        if (bind) {
            tags.add(tag);
        } else {
            tags.remove(tag);
        }
        sendHeartbeat();
    }

    private void mockHandleTask(TaskResult taskResult) {
        taskResult.getDataConfigs().forEach(dataConfig -> {
            CommandEntity command = new CommandEntity();

            // todo:isAck、id is useless, so it can be removed
            command.setCommandResult(Constants.RESULT_SUCCESS);
            command.setTaskId(dataConfig.getTaskId());
            command.setVersion(dataConfig.getVersion());
            commands.offer(command);
        });
    }
}
