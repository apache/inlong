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

package org.apache.inlong.manager.service.workflow.consumption.listener;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.beans.ClusterBean;
import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.enums.ConsumptionStatus;
import org.apache.inlong.manager.common.event.ListenerResult;
import org.apache.inlong.manager.common.event.process.ProcessEvent;
import org.apache.inlong.manager.common.event.process.ProcessEventListener;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.model.WorkflowContext;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.pulsar.PulsarTopicBean;
import org.apache.inlong.manager.common.pojo.tubemq.AddTubeConsumeGroupRequest;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.ConsumptionEntity;
import org.apache.inlong.manager.dao.mapper.ConsumptionEntityMapper;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.thirdpart.mq.PulsarOptService;
import org.apache.inlong.manager.service.thirdpart.mq.TubeMqOptService;
import org.apache.inlong.manager.service.thirdpart.mq.util.PulsarUtils;
import org.apache.inlong.manager.common.workflow.consumption.NewConsumptionWorkflowForm;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Added data consumption process complete archive event listener
 */
@Slf4j
@Component
public class ConsumptionCompleteProcessListener implements ProcessEventListener {

    @Autowired
    private PulsarOptService pulsarMqOptService;
    @Autowired
    private ClusterBean clusterBean;
    @Autowired
    private BusinessService businessService;
    @Autowired
    private ConsumptionEntityMapper consumptionMapper;
    @Autowired
    private TubeMqOptService tubeMqOptService;

    @Override
    public ProcessEvent event() {
        return ProcessEvent.COMPLETE;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        NewConsumptionWorkflowForm consumptionForm = (NewConsumptionWorkflowForm) context.getProcessForm();

        // Real-time query of consumption information
        Integer consumptionId = consumptionForm.getConsumptionInfo().getId();
        ConsumptionEntity entity = consumptionMapper.selectByPrimaryKey(consumptionId);
        if (entity == null) {
            throw new WorkflowListenerException("consumption not exits for id=" + consumptionId);
        }

        String middlewareType = entity.getMiddlewareType();
        if (BizConstant.MIDDLEWARE_TUBE.equalsIgnoreCase(middlewareType)) {
            this.createTubeConsumerGroup(entity);
            return ListenerResult.success("Create Tube consumer group successful");
        } else if (BizConstant.MIDDLEWARE_PULSAR.equalsIgnoreCase(middlewareType)) {
            this.createPulsarTopicMessage(entity);
        } else {
            throw new WorkflowListenerException("middleware type [" + middlewareType + "] not supported");
        }

        this.updateConsumerInfo(consumptionId, entity.getConsumerGroupId());
        return ListenerResult.success("create Tube /Pulsar consumer group successful");
    }

    /**
     * Update consumption after approve
     */
    private void updateConsumerInfo(Integer consumptionId, String consumerGroupId) {
        ConsumptionEntity update = new ConsumptionEntity();
        update.setId(consumptionId);
        update.setStatus(ConsumptionStatus.APPROVED.getStatus());
        update.setConsumerGroupId(consumerGroupId);
        update.setModifyTime(new Date());
        consumptionMapper.updateByPrimaryKeySelective(update);
    }

    /**
     * Create Pulsar consumption information, including cross-regional cycle creation of consumption groups
     */
    private void createPulsarTopicMessage(ConsumptionEntity entity) {
        String groupId = entity.getInlongGroupId();
        BusinessInfo businessInfo = businessService.get(groupId);
        Preconditions.checkNotNull(businessInfo, "business not found for groupId=" + groupId);
        String mqResourceObj = businessInfo.getMqResourceObj();
        Preconditions.checkNotNull(mqResourceObj, "mq resource cannot empty for groupId=" + groupId);

        try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(businessInfo, clusterBean.getPulsarAdminUrl())) {
            PulsarTopicBean topicMessage = new PulsarTopicBean();
            String tenant = clusterBean.getDefaultTenant();
            topicMessage.setTenant(tenant);
            topicMessage.setNamespace(mqResourceObj);

            // If cross-regional replication is started, each cluster needs to create consumer groups in cycles
            String consumerGroup = entity.getConsumerGroupId();
            List<String> clusters = PulsarUtils.getPulsarClusters(pulsarAdmin);
            List<String> topics = Arrays.asList(entity.getTopic().split(","));
            this.createPulsarSubscription(pulsarAdmin, consumerGroup, topicMessage, clusters, topics, businessInfo);
        } catch (Exception e) {
            log.error("create pulsar topic failed", e);
            throw new WorkflowListenerException("failed to create pulsar topic for groupId=" + groupId + ", reason: "
                    + e.getMessage());
        }
    }

    private void createPulsarSubscription(PulsarAdmin globalPulsarAdmin, String subscription, PulsarTopicBean topicBean,
            List<String> clusters, List<String> topics, BusinessInfo businessInfo) {
        try {
            for (String cluster : clusters) {
                String serviceUrl = PulsarUtils.getServiceUrl(globalPulsarAdmin, cluster);
                try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(businessInfo, serviceUrl)) {
                    pulsarMqOptService.createSubscriptions(pulsarAdmin, subscription, topicBean, topics);
                }
            }
        } catch (Exception e) {
            log.error("create pulsar consumer group failed", e);
            throw new WorkflowListenerException("failed to create pulsar consumer group");
        }
    }

    /**
     * Create tube consumer group
     */
    private void createTubeConsumerGroup(ConsumptionEntity consumption) {
        AddTubeConsumeGroupRequest addTubeConsumeGroupRequest = new AddTubeConsumeGroupRequest();
        addTubeConsumeGroupRequest.setClusterId(1); // TODO is cluster id needed?
        addTubeConsumeGroupRequest.setCreateUser(consumption.getCreator());
        AddTubeConsumeGroupRequest.GroupNameJsonSetBean bean = new AddTubeConsumeGroupRequest.GroupNameJsonSetBean();
        bean.setTopicName(consumption.getTopic());
        bean.setGroupName(consumption.getConsumerGroupId());
        addTubeConsumeGroupRequest.setGroupNameJsonSet(Collections.singletonList(bean));

        try {
            tubeMqOptService.createNewConsumerGroup(addTubeConsumeGroupRequest);
        } catch (Exception e) {
            throw new WorkflowListenerException("failed to create tube consumer group: " + addTubeConsumeGroupRequest);
        }
    }

    @Override
    public boolean async() {
        return false;
    }

}
