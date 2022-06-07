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

package org.apache.inlong.manager.service.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.ReTryConfigBean;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.cluster.InlongClusterInfo;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeConsumeGroupRequest;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeConsumeGroupRequest.GroupNameJsonSetBean;
import org.apache.inlong.manager.common.pojo.tubemq.QueryTubeTopicRequest;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.dao.entity.InlongGroupEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupEntityMapper;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.mq.util.TubeMqOptService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.QueueOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * Event listener of create tube consumer group.
 */
@Component
@Slf4j
public class CreateTubeGroupTaskListener implements QueueOperateListener {

    @Autowired
    private InlongGroupEntityMapper groupMapper;
    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private TubeMqOptService tubeMqOptService;
    @Autowired
    private ReTryConfigBean reTryConfigBean;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        GroupResourceProcessForm form = (GroupResourceProcessForm) context.getProcessForm();
        String groupId = form.getInlongGroupId();
        log.info("try to create consumer group for groupId {}", groupId);

        InlongGroupEntity groupEntity = groupMapper.selectByGroupId(groupId);
        InlongClusterInfo tubeCluster = clusterService.getOne(groupEntity.getInlongClusterTag(),
                null, ClusterType.CLS_TUBE);

        // TODO use the original method of TubeMQ to create group
        // TubeClusterDTO clusterDTO = TubeClusterDTO.getFromJson(clusters.get(0).getExtParams());
        // int clusterId = clusterDTO.getClusterId();

        String topicName = groupEntity.getMqResource();
        QueryTubeTopicRequest queryTubeTopicRequest = QueryTubeTopicRequest.builder()
                .topicName(topicName).clusterId(1)
                .user(groupEntity.getCreator()).build();
        // Query whether the tube topic exists
        boolean topicExist = tubeMqOptService.queryTopicIsExist(queryTubeTopicRequest);

        Integer tryNumber = reTryConfigBean.getMaxAttempts();
        Long delay = reTryConfigBean.getDelay();
        while (!topicExist && --tryNumber > 0) {
            log.info("check whether the tube topic exists, try count={}", tryNumber);
            try {
                Thread.sleep(delay);
                delay *= reTryConfigBean.getMultiplier();
                topicExist = tubeMqOptService.queryTopicIsExist(queryTubeTopicRequest);
            } catch (InterruptedException e) {
                log.error("check the tube topic exists error", e);
            }
        }

        AddTubeConsumeGroupRequest addTubeConsumeGroupRequest = new AddTubeConsumeGroupRequest();
        addTubeConsumeGroupRequest.setClusterId(1);
        addTubeConsumeGroupRequest.setCreateUser(groupEntity.getCreator());

        GroupNameJsonSetBean groupNameJsonSetBean = new GroupNameJsonSetBean();
        groupNameJsonSetBean.setTopicName(topicName);
        String consumeGroupName = "sort_" + topicName + "_group";
        groupNameJsonSetBean.setGroupName(consumeGroupName);
        addTubeConsumeGroupRequest.setGroupNameJsonSet(Collections.singletonList(groupNameJsonSetBean));

        try {
            tubeMqOptService.createNewConsumerGroup(addTubeConsumeGroupRequest);
        } catch (Exception e) {
            throw new WorkflowListenerException("create tube consumer group for groupId=" + groupId + " error", e);
        }
        log.info("finish to create consumer group for {}", groupId);
        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return true;
    }

}
