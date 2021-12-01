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
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.pulsar.PulsarTopicBean;
import org.apache.inlong.manager.dao.entity.DataStreamEntity;
import org.apache.inlong.manager.dao.mapper.DataStreamEntityMapper;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.core.ConsumptionService;
import org.apache.inlong.manager.service.core.StorageService;
import org.apache.inlong.manager.service.thirdpart.mq.util.PulsarUtils;
import org.apache.inlong.manager.service.workflow.business.BusinessResourceWorkflowForm;
import org.apache.inlong.manager.workflow.core.event.ListenerResult;
import org.apache.inlong.manager.workflow.core.event.task.TaskEvent;
import org.apache.inlong.manager.workflow.core.event.task.TaskEventListener;
import org.apache.inlong.manager.workflow.exception.WorkflowListenerException;
import org.apache.inlong.manager.workflow.model.WorkflowContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Create a subscription group for a single data stream
 */
@Slf4j
@Component
public class CreatePulsarGroupForStreamTaskListener implements TaskEventListener {

    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private BusinessService businessService;
    @Autowired
    private DataStreamEntityMapper dataStreamMapper;
    @Autowired
    private PulsarOptService pulsarOptService;
    @Autowired
    private StorageService storageService;
    @Autowired
    private ConsumptionService consumptionService;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        BusinessResourceWorkflowForm form = (BusinessResourceWorkflowForm) context.getProcessForm();
        String groupId = form.getInlongGroupId();
        String streamId = form.getInlongStreamId();

        BusinessInfo bizInfo = businessService.get(groupId);
        if (bizInfo == null) {
            log.error("business not found with groupId={}", groupId);
            throw new WorkflowListenerException("business not found with groupId=" + groupId);
        }

        DataStreamEntity streamEntity = dataStreamMapper.selectByIdentifier(groupId, streamId);
        if (streamEntity == null) {
            log.warn("data stream is empty for group={}, stream={}, skip to create pulsar group", groupId, streamId);
            return ListenerResult.success();
        }

        try (PulsarAdmin globalPulsarAdmin = PulsarUtils.getPulsarAdmin(clusterBean.getPulsarAdminUrl())) {
            // Query data storage info based on groupId and streamId
            List<String> storageTypeList = storageService.getStorageTypeList(groupId, streamId);
            if (storageTypeList == null || storageTypeList.size() == 0) {
                log.warn("storage info is empty for groupId={}, streamId={}, skip to create pulsar group",
                        groupId, streamId);
                return ListenerResult.success();
            }

            PulsarTopicBean topicBean = new PulsarTopicBean();
            topicBean.setTenant(clusterBean.getDefaultTenant());
            topicBean.setNamespace(bizInfo.getMqResourceObj());
            String topic = streamEntity.getMqResourceObj();
            topicBean.setTopicName(topic);
            List<String> pulsarClusters = PulsarUtils.getPulsarClusters(globalPulsarAdmin);

            // Create a subscription in the Pulsar cluster (cross-region), you need to ensure that the Topic exists
            String tenant = clusterBean.getDefaultTenant();
            String namespace = bizInfo.getMqResourceObj();
            for (String cluster : pulsarClusters) {
                String serviceUrl = PulsarUtils.getServiceUrl(globalPulsarAdmin, cluster);
                try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(serviceUrl)) {
                    boolean exist = pulsarOptService.topicIsExists(pulsarAdmin, tenant, namespace, topic);
                    if (!exist) {
                        String fullTopic = tenant + "/" + namespace + "/" + topic;
                        log.error("topic={} not exists in {}", fullTopic, pulsarAdmin.getServiceUrl());
                        throw new BusinessException("topic=" + fullTopic + " not exists in " + serviceUrl);
                    }

                    // Consumer naming rules: sortAppName_topicName_consumer_group
                    String subscription = clusterBean.getAppName() + "_" + topic + "_consumer_group";
                    pulsarOptService.createSubscription(pulsarAdmin, topicBean, subscription);

                    // Insert the consumption data into the consumption table
                    consumptionService.saveSortConsumption(bizInfo, topic, subscription);
                }
            }
        } catch (Exception e) {
            log.error("create pulsar subscription error for groupId={}, streamId={}", groupId, streamId, e);
            throw new WorkflowListenerException("create pulsar subscription error, reason: " + e.getMessage());
        }

        log.info("finish to create single pulsar subscription for groupId={}, streamId={}", groupId, streamId);
        return ListenerResult.success();
    }

    @Override
    public boolean async() {
        return false;
    }

}
