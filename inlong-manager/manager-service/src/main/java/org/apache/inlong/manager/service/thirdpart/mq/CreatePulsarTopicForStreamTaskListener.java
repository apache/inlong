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

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.event.ListenerResult;
import org.apache.inlong.manager.common.event.task.QueueOperateListener;
import org.apache.inlong.manager.common.event.task.TaskEvent;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.pulsar.PulsarTopicBean;
import org.apache.inlong.manager.common.workflow.bussiness.BusinessResourceWorkflowForm;
import org.apache.inlong.manager.dao.entity.DataStreamEntity;
import org.apache.inlong.manager.dao.mapper.DataStreamEntityMapper;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.thirdpart.mq.util.PulsarUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Create task listener for Pulsar Topic
 */
@Slf4j
@Component
public class CreatePulsarTopicForStreamTaskListener implements QueueOperateListener {

    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private PulsarOptService pulsarOptService;
    @Autowired
    private BusinessService businessService;
    @Autowired
    private DataStreamEntityMapper dataStreamMapper;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) context.getProcessForm();
        String groupId = form.getInlongGroupId();
        String streamId = form.getInlongStreamId();

        BusinessInfo businessInfo = businessService.get(groupId);
        DataStreamEntity streamEntity = dataStreamMapper.selectByIdentifier(groupId, streamId);
        if (businessInfo == null || streamEntity == null) {
            throw new WorkflowListenerException("business or data stream not found with groupId=" + groupId
                    + ", streamId=" + streamId);
        }

        log.info("begin to create pulsar topic for groupId={}, streamId={}", groupId, streamId);
        try (PulsarAdmin globalPulsarAdmin = PulsarUtils.getPulsarAdmin(businessInfo,
                clusterBean.getPulsarAdminUrl())) {
            List<String> pulsarClusters = PulsarUtils.getPulsarClusters(globalPulsarAdmin);
            for (String cluster : pulsarClusters) {
                String serviceUrl = PulsarUtils.getServiceUrl(globalPulsarAdmin, cluster);
                String pulsarTopic = streamEntity.getMqResourceObj();
                this.createTopicOpt(businessInfo, pulsarTopic, serviceUrl);
            }
        } catch (Exception e) {
            log.error("create pulsar topic error for groupId={}, streamId={}", groupId, streamId, e);
            throw new WorkflowListenerException(
                    "create pulsar topic error for groupId=" + groupId + ", streamId=" + streamId);
        }

        log.info("success to create pulsar topic for groupId={}, streamId={}", groupId, streamId);
        return ListenerResult.success();
    }

    private void createTopicOpt(BusinessInfo bizInfo, String pulsarTopic, String serviceHttpUrl) throws Exception {
        Integer partitionNum = bizInfo.getTopicPartitionNum();
        int partition = 0;
        if (partitionNum != null && partitionNum > 0) {
            partition = partitionNum;
        }

        try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(bizInfo, serviceHttpUrl)) {
            PulsarTopicBean topicBean = PulsarTopicBean.builder()
                    .tenant(clusterBean.getDefaultTenant())
                    .namespace(bizInfo.getMqResourceObj())
                    .topicName(pulsarTopic)
                    .numPartitions(partition)
                    .queueModule(bizInfo.getQueueModule())
                    .build();
            pulsarOptService.createTopic(pulsarAdmin, topicBean);
        }
    }

    @Override
    public boolean async() {
        return false;
    }
}
