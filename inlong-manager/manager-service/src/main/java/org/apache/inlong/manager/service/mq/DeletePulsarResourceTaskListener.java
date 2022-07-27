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
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.GroupOperateType;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.common.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.common.pojo.pulsar.PulsarTopicBean;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamBriefInfo;
import org.apache.inlong.manager.common.pojo.workflow.form.process.GroupResourceProcessForm;
import org.apache.inlong.manager.common.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.mq.util.PulsarOperator;
import org.apache.inlong.manager.service.mq.util.PulsarUtils;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.QueueOperateListener;
import org.apache.inlong.manager.workflow.event.task.TaskEvent;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Delete Pulsar tenant, namespace and topic after group is operated to delete
 */
@Slf4j
@Component
public class DeletePulsarResourceTaskListener implements QueueOperateListener {

    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private PulsarOperator pulsarOperator;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public boolean accept(WorkflowContext context) {
        ProcessForm processForm = context.getProcessForm();
        if (!(processForm instanceof GroupResourceProcessForm)) {
            return false;
        }
        GroupResourceProcessForm form = (GroupResourceProcessForm) processForm;
        GroupOperateType operateType = form.getGroupOperateType();
        if (operateType != GroupOperateType.DELETE) {
            return false;
        }
        String groupId = form.getInlongGroupId();
        InlongGroupInfo groupInfo = form.getGroupInfo();
        MQType mqType = MQType.forType(groupInfo.getMqType());
        if (mqType == MQType.PULSAR || mqType == MQType.TDMQ_PULSAR) {
            InlongPulsarInfo pulsarInfo = (InlongPulsarInfo) groupInfo;
            boolean enable = InlongConstants.ENABLE_CREATE_RESOURCE.equals(pulsarInfo.getEnableCreateResource());
            if (enable) {
                log.info("need to delete pulsar resource as the createResource was true for groupId [{}]", groupId);
                return true;
            } else {
                log.info("skip to delete pulsar resource as the createResource was false for groupId [{}]", groupId);
                return false;
            }
        }

        log.warn("skip to delete pulsar subscription as the mq type is {} for groupId [{}]", mqType, groupId);
        return false;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws Exception {
        GroupResourceProcessForm form = (GroupResourceProcessForm) context.getProcessForm();
        String groupId = form.getInlongGroupId();
        log.info("begin to delete pulsar resource for groupId={}", groupId);
        InlongGroupInfo groupInfo = groupService.get(groupId);
        if (groupInfo == null) {
            throw new WorkflowListenerException("inlong group or pulsar cluster not found for groupId=" + groupId);
        }
        try {
            InlongPulsarInfo pulsarInfo = (InlongPulsarInfo) groupInfo;
            ClusterInfo clusterInfo = clusterService.getOne(groupInfo.getInlongClusterTag(), null,
                    ClusterType.PULSAR);
            deletePulsarProcess(pulsarInfo, (PulsarClusterInfo) clusterInfo);
        } catch (Exception e) {
            log.error("delete pulsar resource error for groupId={}", groupId, e);
            throw new WorkflowListenerException("delete pulsar resource error for groupId=" + groupId);
        }

        log.info("success to delete pulsar resource for groupId={}", groupId);
        return ListenerResult.success();
    }

    /**
     * Delete all Pulsar topics forcefully
     */
    private void deletePulsarProcess(InlongPulsarInfo pulsarInfo, PulsarClusterInfo pulsarCluster) throws Exception {
        String groupId = pulsarInfo.getInlongGroupId();
        log.info("begin to delete pulsar resource for groupId={} in cluster={}", groupId, pulsarCluster);
        final String tenant = pulsarCluster.getTenant();
        final String namespace = pulsarInfo.getMqResource();
        Preconditions.checkNotNull(namespace, "pulsar namespace cannot be empty for groupId=" + groupId);
        try (PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(pulsarCluster)) {
            List<InlongStreamBriefInfo> streamTopicList = streamService.getTopicList(groupId);
            for (InlongStreamBriefInfo streamInfo : streamTopicList) {
                final String topic = streamInfo.getMqResource();
                if (topic == null) {
                    continue;
                }
                PulsarTopicBean topicBean = PulsarTopicBean.builder()
                        .tenant(tenant)
                        .namespace(namespace)
                        .topicName(topic)
                        .build();
                pulsarOperator.forceDeleteTopic(pulsarAdmin, topicBean);
            }
        }
        log.info("finish to delete pulsar resource for groupId={}, cluster={}", groupId, pulsarCluster);
    }
}
