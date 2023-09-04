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

package org.apache.inlong.manager.service.resource.sink.pulsar;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.node.pulsar.PulsarDataNodeInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTopicInfo;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.sink.pulsar.PulsarSinkDTO;
import org.apache.inlong.manager.service.node.DataNodeOperateHelper;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarOperator;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarUtils;
import org.apache.inlong.manager.service.resource.sink.SinkResourceOperator;
import org.apache.inlong.manager.service.sink.StreamSinkService;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * pulsar resource operate for creating pulsar resource
 */
@Service
public class PulsarResourceOperator implements SinkResourceOperator {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarResourceOperator.class);
    @Autowired
    private StreamSinkService sinkService;

    @Autowired
    private DataNodeOperateHelper dataNodeHelper;
    @Autowired
    private PulsarOperator pulsarOperator;

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.PULSAR.equals(sinkType);
    }

    @Override
    public void createSinkResource(SinkInfo sinkInfo) {
        LOG.info("begin to create sink resources sinkId={}", sinkInfo.getId());
        if (SinkStatus.CONFIG_SUCCESSFUL.getCode().equals(sinkInfo.getStatus())) {
            LOG.warn("sink resource [" + sinkInfo.getId() + "] already success, skip to create");
            return;
        } else if (InlongConstants.DISABLE_CREATE_RESOURCE.equals(sinkInfo.getEnableCreateResource())) {
            LOG.warn("create resource was disabled, skip to create for [" + sinkInfo.getId() + "]");
            return;
        }
        this.createTopic(sinkInfo);
    }

    private void createTopic(SinkInfo sinkInfo) {
        PulsarSinkDTO pulsarSinkDTO = getPulsarDataNodeInfo(sinkInfo);
        try {
            PulsarClusterInfo pulsarClusterInfo = PulsarClusterInfo.builder().adminUrl(pulsarSinkDTO.getAdminUrl())
                    .token(pulsarSinkDTO.getToken()).build();
            PulsarAdmin pulsarAdmin = PulsarUtils.getPulsarAdmin(pulsarClusterInfo);
            String queueModel = pulsarSinkDTO.getPartitionNum() > 0 ? InlongConstants.PULSAR_QUEUE_TYPE_PARALLEL
                    : InlongConstants.PULSAR_QUEUE_TYPE_SERIAL;
            PulsarTopicInfo topicInfo = PulsarTopicInfo.builder().pulsarTenant(pulsarSinkDTO.getTenant())
                    .namespace(pulsarSinkDTO.getNamespace())
                    .topicName(pulsarSinkDTO.getTopic())
                    .numPartitions(pulsarSinkDTO.getPartitionNum())
                    .queueModule(queueModel)
                    .build();
            pulsarOperator.createTopic(pulsarAdmin, topicInfo);
        } catch (PulsarClientException | PulsarAdminException e) {
            LOG.error("init pulsar admin error", e);
            throw new BusinessException();
        }

    }

    private PulsarSinkDTO getPulsarDataNodeInfo(SinkInfo sinkInfo) {
        PulsarSinkDTO pulsarSinkDTO = PulsarSinkDTO.getFromJson(sinkInfo.getExtParams());
        // read from data node if not supplied by user
        if (StringUtils.isBlank(pulsarSinkDTO.getAdminUrl())) {
            String dataNodeName = sinkInfo.getDataNodeName();
            Preconditions.expectNotBlank(dataNodeName, ErrorCodeEnum.INVALID_PARAMETER,
                    "pulsar admin url not specified and data node is empty");
            PulsarDataNodeInfo dataNodeInfo = (PulsarDataNodeInfo) dataNodeHelper.getDataNodeInfo(
                    dataNodeName, sinkInfo.getSinkType());
            CommonBeanUtils.copyProperties(dataNodeInfo, pulsarSinkDTO);
        }
        return pulsarSinkDTO;
    }
}
