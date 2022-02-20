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

package org.apache.inlong.manager.service.thirdpart.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.ReTryConfigBean;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeConsumeGroupRequest;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeConsumeGroupRequest.GroupNameJsonSetBean;
import org.apache.inlong.manager.common.pojo.tubemq.QueryTubeTopicRequest;
import org.apache.inlong.manager.dao.mapper.ClusterInfoMapper;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.common.pojo.workflow.form.BusinessResourceProcessForm;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.QueueOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Component
@Slf4j
public class CreateTubeGroupTaskListener implements QueueOperateListener {

    @Autowired
    BusinessService businessService;

    @Autowired
    ClusterInfoMapper clusterInfoMapper;

    @Autowired
    TubeMqOptService tubeMqOptService;

    @Value("${cluster.tube.clusterId}")
    Integer clusterId;

    @Autowired
    ReTryConfigBean reTryConfigBean;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        BusinessResourceProcessForm form = (BusinessResourceProcessForm) context.getProcessForm();
        String groupId = form.getInlongGroupId();
        log.info("try to create consumer group for groupId {}", groupId);

        BusinessInfo businessInfo = businessService.get(groupId);
        String topicName = businessInfo.getMqResourceObj();
        QueryTubeTopicRequest queryTubeTopicRequest = QueryTubeTopicRequest.builder()
                .topicName(topicName).clusterId(clusterId)
                .user(businessInfo.getCreator()).build();
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
        addTubeConsumeGroupRequest.setClusterId(clusterId);
        addTubeConsumeGroupRequest.setCreateUser(businessInfo.getCreator());

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
