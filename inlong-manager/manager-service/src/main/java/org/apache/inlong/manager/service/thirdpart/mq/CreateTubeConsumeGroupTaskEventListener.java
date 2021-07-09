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

import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.TryBean;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeConsumeGroupRequest;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeConsumeGroupRequest.GroupNameJsonSetBean;
import org.apache.inlong.manager.common.pojo.tubemq.QueryTubeTopicRequest;
import org.apache.inlong.manager.dao.mapper.ClusterInfoMapper;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.workflow.newbusiness.CreateResourceWorkflowForm;
import org.apache.inlong.manager.workflow.core.event.ListenerResult;
import org.apache.inlong.manager.workflow.core.event.task.TaskEvent;
import org.apache.inlong.manager.workflow.core.event.task.TaskEventListener;
import org.apache.inlong.manager.workflow.exception.WorkflowListenerException;
import org.apache.inlong.manager.workflow.model.WorkflowContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class CreateTubeConsumeGroupTaskEventListener implements TaskEventListener {

    @Autowired
    BusinessService businessService;

    @Autowired
    ClusterInfoMapper clusterInfoMapper;

    @Autowired
    TubeMqOptService tubeMqOptService;

    @Value("${cluster.tube.clusterId}")
    Integer clusterId;

    @Autowired
    TryBean tryBean;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }


    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        CreateResourceWorkflowForm form = (CreateResourceWorkflowForm) context.getProcessForm();
        String bid = form.getBusinessId();

        log.info("try to create consumer group for bid {}", bid);

        BusinessInfo businessInfo = businessService.get(bid);

        String topicName = businessInfo.getMqResourceObj();

        QueryTubeTopicRequest queryTubeTopicRequest = QueryTubeTopicRequest.builder()
                .topicName(topicName).clusterId(clusterId)
                .user(businessInfo.getCreator()).build();
        // Query whether the tube topic exists
        boolean topicExist = tubeMqOptService.queryTopicIsExist(queryTubeTopicRequest);

        Integer tryNumber = tryBean.getMaxAttempts();
        Long delay = tryBean.getDelay();
        while (--tryNumber > 0) {
            if (topicExist) {
                break;
            }

            try {
                Thread.sleep(delay);
                delay *= tryBean.getMultiplier();
                topicExist = tubeMqOptService.queryTopicIsExist(queryTubeTopicRequest);
            } catch (InterruptedException e) {
                log.error("Try to determine whether the tube topic exists {}", e.getMessage());
            }

        }
        log.info("Try to determine whether the tube topic exists ,try number is {}", tryNumber);
        AddTubeConsumeGroupRequest addTubeConsumeGroupRequest = new AddTubeConsumeGroupRequest();
        addTubeConsumeGroupRequest.setClusterId(clusterId);
        addTubeConsumeGroupRequest.setCreateUser(businessInfo.getCreator());

        GroupNameJsonSetBean groupNameJsonSetBean = new GroupNameJsonSetBean();
        groupNameJsonSetBean.setTopicName(topicName);
        String consumeGroupName = "sort_" + topicName + "_consumer_group";
        groupNameJsonSetBean.setGroupName(consumeGroupName);
        addTubeConsumeGroupRequest.setGroupNameJsonSet(Collections.singletonList(groupNameJsonSetBean));

        try {
            tubeMqOptService.createNewConsumerGroup(addTubeConsumeGroupRequest);
        } catch (Exception e) {
            log.error("create tube consume group for bid={} error {}", bid, e.getMessage(), e);
        }
        log.info("finish to create consumer group for {}", bid);
        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }
}
