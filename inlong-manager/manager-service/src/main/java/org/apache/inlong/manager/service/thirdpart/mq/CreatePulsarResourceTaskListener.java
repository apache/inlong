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
import org.apache.inlong.manager.common.pojo.datastream.DataStreamTopicVO;
import org.apache.inlong.manager.common.pojo.pulsar.PulsarTopicBean;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.BusinessPulsarEntity;
import org.apache.inlong.manager.dao.mapper.BusinessPulsarEntityMapper;
import org.apache.inlong.manager.dao.mapper.DataStreamEntityMapper;
import org.apache.inlong.manager.service.core.BusinessService;
import org.apache.inlong.manager.service.thirdpart.mq.util.PulsarUtils;
import org.apache.inlong.manager.service.workflow.business.BusinessResourceWorkflowForm;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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
    private BusinessService businessService;
    @Autowired
    private BusinessPulsarEntityMapper businessPulsarMapper;
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
        log.info("begin to create pulsar resource for groupId={}", groupId);

        BusinessInfo businessInfo = businessService.get(groupId);
        if (businessInfo == null) {
            throw new WorkflowListenerException("business or pulsar cluster not found for groupId=" + groupId);
        }

        try (PulsarAdmin globalPulsarAdmin = PulsarUtils.getPulsarAdmin(clusterBean.getPulsarAdminUrl())) {
            List<String> pulsarClusters = PulsarUtils.getPulsarClusters(globalPulsarAdmin);
            for (String cluster : pulsarClusters) {
                String serviceUrl = PulsarUtils.getServiceUrl(globalPulsarAdmin, cluster);
                this.createPulsarProcess(businessInfo, serviceUrl);
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
    private void createPulsarProcess(BusinessInfo businessInfo, String serviceHttpUrl) throws Exception {
        String groupId = businessInfo.getInlongGroupId();
        log.info("begin to create pulsar resource for groupId={} in cluster={}", groupId, serviceHttpUrl);

        String namespace = businessInfo.getMqResourceObj();
        Preconditions.checkNotNull(namespace, "pulsar namespace cannot be empty for groupId=" + groupId);
        String queueModule = businessInfo.getQueueModule();
        Preconditions.checkNotNull(queueModule, "queue module cannot be empty for groupId=" + groupId);

        String tenant = clusterBean.getDefaultTenant();
        try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(serviceHttpUrl)) {
            // create pulsar tenant
            pulsarOptService.createTenant(pulsarAdmin, tenant);

            // create pulsar namespace
            BusinessPulsarEntity entity = businessPulsarMapper.selectByGroupId(groupId);
            pulsarOptService.createNamespace(pulsarAdmin, entity, tenant, namespace);

            // create pulsar topic
            Integer partitionNum = businessInfo.getTopicPartitionNum();
            List<DataStreamTopicVO> streamTopicList = dataStreamMapper.selectTopicList(groupId);
            PulsarTopicBean topicBean = PulsarTopicBean.builder()
                    .tenant(tenant).namespace(namespace).numPartitions(partitionNum).queueModule(queueModule).build();

            for (DataStreamTopicVO topicVO : streamTopicList) {
                topicBean.setTopicName(topicVO.getMqResourceObj());
                pulsarOptService.createTopic(pulsarAdmin, topicBean);
            }
        }
        log.info("finish to create pulsar resource for groupId={}, service http url={}", groupId, serviceHttpUrl);
    }

    @Override
    public boolean async() {
        return false;
    }

}
