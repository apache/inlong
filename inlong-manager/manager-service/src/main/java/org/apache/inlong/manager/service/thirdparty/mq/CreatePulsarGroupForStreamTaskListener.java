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

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.pulsar.PulsarTopicBean;
import org.apache.inlong.manager.common.pojo.workflow.form.GroupResourceProcessForm;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.service.CommonOperateService;
import org.apache.inlong.manager.service.core.ConsumptionService;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.apache.inlong.manager.service.thirdparty.mq.util.PulsarUtils;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.QueueOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Create a subscription group for a single inlong stream
 */
@Slf4j
@Component
public class CreatePulsarGroupForStreamTaskListener implements QueueOperateListener {

    @Autowired
    private CommonOperateService commonOperateService;
    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private InlongStreamEntityMapper streamMapper;
    @Autowired
    private PulsarOptService pulsarOptService;
    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private ConsumptionService consumptionService;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        GroupResourceProcessForm form = (GroupResourceProcessForm) context.getProcessForm();
        String groupId = form.getInlongGroupId();
        String streamId = form.getInlongStreamId();

        InlongGroupInfo bizInfo = groupService.get(groupId);
        if (bizInfo == null) {
            log.error("inlong group not found with groupId={}", groupId);
            throw new WorkflowListenerException("inlong group not found with groupId=" + groupId);
        }

        InlongStreamEntity streamEntity = streamMapper.selectByIdentifier(groupId, streamId);
        if (streamEntity == null) {
            log.warn("inlong stream is empty for group={}, stream={}, skip to create pulsar group", groupId, streamId);
            return ListenerResult.success();
        }

        try (PulsarAdmin globalPulsarAdmin = PulsarUtils
                .getPulsarAdmin(bizInfo, commonOperateService.getSpecifiedParam(Constant.PULSAR_ADMINURL))) {
            // Query data sink info based on groupId and streamId
            List<String> sinkTypeList = sinkService.getSinkTypeList(groupId, streamId);
            if (sinkTypeList == null || sinkTypeList.size() == 0) {
                log.warn("sink info is empty for groupId={}, streamId={}, skip to create pulsar group",
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
                try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(bizInfo, serviceUrl)) {
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
