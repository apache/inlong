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

package org.apache.inlong.manager.service.listener.queue;

import org.apache.inlong.common.pojo.sort.mq.PulsarClusterConfig;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.TaskEvent;
import org.apache.inlong.manager.common.exceptions.WorkflowListenerException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.dao.entity.ClusterConfigEntity;
import org.apache.inlong.manager.dao.mapper.ClusterConfigEntityMapper;
import org.apache.inlong.manager.pojo.cluster.ClusterInfo;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.workflow.form.process.ClusterResourceProcessForm;
import org.apache.inlong.manager.pojo.workflow.form.process.ProcessForm;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.workflow.WorkflowContext;
import org.apache.inlong.manager.workflow.event.ListenerResult;
import org.apache.inlong.manager.workflow.event.task.ClusterOperateListener;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Create cluster config listener for cluster tag
 */
@Slf4j
@Component
public class ClusterConfigListener implements ClusterOperateListener {

    @Autowired
    private InlongClusterService clusterService;
    @Autowired
    private ClusterConfigEntityMapper clusterConfigEntityMapper;

    @Override
    public TaskEvent event() {
        return TaskEvent.COMPLETE;
    }

    @Override
    public boolean accept(WorkflowContext context) {
        ProcessForm processForm = context.getProcessForm();
        return processForm instanceof ClusterResourceProcessForm;
    }

    @Override
    public ListenerResult listen(WorkflowContext context) throws WorkflowListenerException {
        ClusterResourceProcessForm clusterProcessForm = (ClusterResourceProcessForm) context.getProcessForm();
        log.info("begin to execute ClusterConfigListener for clusterTag={}", clusterProcessForm.getInlongClusterTag());
        ObjectMapper objectMapper = new ObjectMapper();
        String clusterTag = clusterProcessForm.getInlongClusterTag();
        String operator = context.getOperator();
        try {
            List<ClusterInfo> clusterInfos = clusterService.listByTagAndType(clusterTag, ClusterType.PULSAR);
            List<PulsarClusterConfig> list = new ArrayList<>();
            ClusterConfigEntity clusterConfigEntity = clusterConfigEntityMapper.selectByClusterTag(clusterTag);
            if (clusterConfigEntity == null) {
                clusterConfigEntity = new ClusterConfigEntity();
                for (ClusterInfo clusterInfo : clusterInfos) {
                    PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterInfo;

                    PulsarClusterConfig pulsarClusterConfig =
                            CommonBeanUtils.copyProperties(pulsarCluster, PulsarClusterConfig::new);
                    pulsarClusterConfig.setVersion(String.valueOf(pulsarCluster.getVersion()));
                    pulsarClusterConfig.setClusterName(pulsarCluster.getName());
                    list.add(pulsarClusterConfig);
                }
                clusterConfigEntity.setConfigParams(objectMapper.writeValueAsString(list));
                clusterConfigEntity.setClusterTag(clusterTag);
                clusterConfigEntity.setClusterType(ClusterType.PULSAR);
                clusterConfigEntity.setModifier(operator);
                clusterConfigEntity.setCreator(operator);
                clusterConfigEntity.setVersion(0);
                clusterConfigEntityMapper.insert(clusterConfigEntity);
            } else {
                for (ClusterInfo clusterInfo : clusterInfos) {
                    PulsarClusterInfo pulsarCluster = (PulsarClusterInfo) clusterInfo;

                    PulsarClusterConfig pulsarClusterConfig =
                            CommonBeanUtils.copyProperties(pulsarCluster, PulsarClusterConfig::new);
                    pulsarClusterConfig.setClusterName(pulsarCluster.getName());
                    pulsarClusterConfig.setVersion(String.valueOf(pulsarCluster.getVersion()));
                    list.add(pulsarClusterConfig);
                }
                clusterConfigEntity.setConfigParams(objectMapper.writeValueAsString(list));
                clusterConfigEntity.setClusterTag(clusterTag);
                clusterConfigEntity.setClusterType(ClusterType.PULSAR);
                clusterConfigEntity.setModifier(operator);
                clusterConfigEntityMapper.updateByIdSelective(clusterConfigEntity);
            }
        } catch (Exception e) {
            String errMsg = String.format("push cluster config failed for cluster Tag=%s", clusterTag);
            log.error(errMsg, e);
            throw new WorkflowListenerException(errMsg);
        }

        log.info("success to execute ClusterConfigListener for clusterTag={}",
                clusterProcessForm.getInlongClusterTag());
        return ListenerResult.success("success");
    }

}
