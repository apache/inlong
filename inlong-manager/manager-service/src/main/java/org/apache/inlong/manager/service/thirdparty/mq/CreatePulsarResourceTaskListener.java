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

package org.apache.inlong.manager.service.thirdparty.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.pulsar.PulsarTopicBean;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamTopicResponse;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongGroupPulsarEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupPulsarEntityMapper;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.service.CommonOperateService;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.apache.inlong.manager.service.thirdparty.mq.util.PulsarUtils;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.QueueOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Create Pulsar tenant, namespace and topic
 */
@Slf4j
@Component()
public class CreatePulsarResourceTaskListener implements QueueOperateListener {

    @Autowired
    PulsarOptService pulsarOptService;
    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private CommonOperateService commonOperateService;
    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private InlongGroupPulsarEntityMapper groupPulsarMapper;
    @Autowired
    private InlongStreamEntityMapper streamMapper;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        GroupResourceProcessForm form = (GroupResourceProcessForm) context.getProcessForm();
        String groupId = form.getInlongGroupId();
        log.info("begin to create pulsar resource for groupId={}", groupId);

        InlongGroupInfo groupInfo = groupService.get(groupId);
        if (groupInfo == null) {
            throw new WorkflowListenerException("inlong group or pulsar cluster not found for groupId=" + groupId);
        }
        PulsarClusterInfo globalCluster = commonOperateService.getPulsarClusterInfo(groupInfo.getMiddlewareType());
        try (PulsarAdmin globalPulsarAdmin = PulsarUtils.getPulsarAdmin(globalCluster)) {
            List<String> pulsarClusters = PulsarUtils.getPulsarClusters(globalPulsarAdmin);
            for (String cluster : pulsarClusters) {
                String serviceUrl = PulsarUtils.getServiceUrl(globalPulsarAdmin, cluster);
                PulsarClusterInfo pulsarClusterInfo = PulsarClusterInfo.builder()
                        .token(globalCluster.getToken()).adminUrl(serviceUrl).build();
                this.createPulsarProcess(groupInfo, pulsarClusterInfo);
            }
        } catch (Exception e) {
            log.error("create pulsar resource error for groupId={}", groupId, e);
            throw new WorkflowListenerException("create pulsar resource error for groupId=" + groupId);
        }

        log.info("success to create pulsar resource for groupId={}", groupId);
        return ListenerResult.success();
    }

    /**
     * Create Pulsar tenant, namespace and topic
     */
    private void createPulsarProcess(InlongGroupInfo groupInfo, PulsarClusterInfo pulsarClusterInfo) throws Exception {
        String groupId = groupInfo.getInlongGroupId();
        log.info("begin to create pulsar resource for groupId={} in cluster={}", groupId, pulsarClusterInfo);

        String namespace = groupInfo.getMqResourceObj();
        Preconditions.checkNotNull(namespace, "pulsar namespace cannot be empty for groupId=" + groupId);
        String queueModule = groupInfo.getQueueModule();
        Preconditions.checkNotNull(queueModule, "queue module cannot be empty for groupId=" + groupId);

        String tenant = clusterBean.getDefaultTenant();
        try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(pulsarClusterInfo)) {
            // create pulsar tenant
            pulsarOptService.createTenant(pulsarAdmin, tenant);

            // create pulsar namespace
            InlongGroupPulsarEntity entity = groupPulsarMapper.selectByGroupId(groupId);
            pulsarOptService.createNamespace(pulsarAdmin, entity, tenant, namespace);

            // create pulsar topic
            Integer partitionNum = groupInfo.getTopicPartitionNum();
            List<InlongStreamTopicResponse> streamTopicList = streamMapper.selectTopicList(groupId);
            PulsarTopicBean topicBean = PulsarTopicBean.builder()
                    .tenant(tenant).namespace(namespace).numPartitions(partitionNum).queueModule(queueModule).build();

            for (InlongStreamTopicResponse topicVO : streamTopicList) {
                topicBean.setTopicName(topicVO.getMqResourceObj());
                pulsarOptService.createTopic(pulsarAdmin, topicBean);
            }
        }
        log.info("finish to create pulsar resource for groupId={}, service http url={}", groupId,
                pulsarClusterInfo.getAdminUrl());
    }

    @Override
    public boolean async() {
        return false;
    }

}
