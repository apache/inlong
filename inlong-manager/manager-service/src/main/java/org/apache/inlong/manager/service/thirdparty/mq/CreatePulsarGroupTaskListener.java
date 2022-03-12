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
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.service.CommonOperateService;
import org.apache.inlong.manager.service.core.ConsumptionService;
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
 * Create default subscription group for Pulsar
 */
@Component
@Slf4j
public class CreatePulsarGroupTaskListener implements QueueOperateListener {

    @Autowired
    private CommonOperateService commonOperateService;
    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private ConsumptionService consumptionService;
    @Autowired
    private InlongStreamEntityMapper streamMapper;
    @Autowired
    private PulsarOptService pulsarOptService;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        GroupResourceProcessForm form = (GroupResourceProcessForm) context.getProcessForm();

        String groupId = form.getInlongGroupId();
        InlongGroupInfo groupInfo = groupService.get(groupId);
        if (groupInfo == null) {
            log.error("inlong group not found with groupId={}", groupId);
            throw new WorkflowListenerException("inlong group not found with groupId=" + groupId);
        }

        // For Pulsar, each Stream corresponds to a Topic
        List<InlongStreamEntity> streamEntities = streamMapper.selectByGroupId(groupId);
        if (streamEntities == null || streamEntities.isEmpty()) {
            log.warn("inlong stream is empty for groupId={}, skip to create pulsar subscription", groupId);
            return ListenerResult.success();
        }
        PulsarClusterInfo globalCluster = commonOperateService.getPulsarClusterInfo(groupInfo.getMiddlewareType());
        try (PulsarAdmin globalPulsarAdmin = PulsarUtils.getPulsarAdmin(globalCluster)) {
            String tenant = clusterBean.getDefaultTenant();
            String namespace = groupInfo.getMqResourceObj();

            for (InlongStreamEntity streamEntity : streamEntities) {
                PulsarTopicBean topicBean = new PulsarTopicBean();
                topicBean.setTenant(tenant);
                topicBean.setNamespace(namespace);
                String topic = streamEntity.getMqResourceObj();
                topicBean.setTopicName(topic);
                List<String> pulsarClusters = PulsarUtils.getPulsarClusters(globalPulsarAdmin);

                // Create a subscription in the Pulsar cluster (cross-region), you need to ensure that the Topic exists
                for (String cluster : pulsarClusters) {
                    String serviceUrl = PulsarUtils.getServiceUrl(globalPulsarAdmin, cluster);
                    PulsarClusterInfo pulsarClusterInfo = PulsarClusterInfo.builder()
                            .token(globalCluster.getToken()).adminUrl(serviceUrl).build();
                    try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(pulsarClusterInfo)) {
                        boolean exist = pulsarOptService.topicIsExists(pulsarAdmin, tenant, namespace, topic);

                        if (!exist) {
                            String topicFull = tenant + "/" + namespace + "/" + topic;
                            log.error("topic={} not exists in {}", topicFull, serviceUrl);
                            throw new WorkflowListenerException("topic=" + topicFull + " not exists in " + serviceUrl);
                        }

                        // Consumer naming rules: sortAppName_topicName_consumer_group
                        String subscription = clusterBean.getAppName() + "_" + topic + "_consumer_group";
                        pulsarOptService.createSubscription(pulsarAdmin, topicBean, subscription);

                        // Insert the consumption data into the consumption table
                        consumptionService.saveSortConsumption(groupInfo, topic, subscription);
                    }
                }
            }
        } catch (Exception e) {
            log.error("create pulsar subscription error for groupId={}", groupId);
            throw new WorkflowListenerException("create pulsar subscription error: " + e.getMessage());
        }

        log.info("success to create pulsar subscription for groupId={}", groupId);
        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }

}
