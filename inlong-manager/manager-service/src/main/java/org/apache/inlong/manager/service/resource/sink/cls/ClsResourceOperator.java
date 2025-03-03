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

package org.apache.inlong.manager.service.resource.sink.cls;

import org.apache.inlong.manager.common.consts.DataNodeType;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.consts.SinkType;
import org.apache.inlong.manager.common.enums.SinkStatus;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.node.cls.ClsDataNodeDTO;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.sink.cls.ClsSinkDTO;
import org.apache.inlong.manager.service.resource.sink.AbstractStandaloneSinkResourceOperator;
import org.apache.inlong.manager.service.sink.StreamSinkService;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
public class ClsResourceOperator extends AbstractStandaloneSinkResourceOperator {

    private static final Logger LOG = LoggerFactory.getLogger(ClsResourceOperator.class);

    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;
    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private StreamSinkEntityMapper streamSinkEntityMapper;
    @Autowired
    private ClsOperator clsOperator;

    @Override
    public Boolean accept(String sinkType) {
        return SinkType.CLS.equals(sinkType);
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
        this.checkTaskAndConsumerGroup(sinkInfo);
        this.createClsResource(sinkInfo);
        this.assignCluster(sinkInfo);
    }

    /**
     * Create cloud log service topic
     */
    private void createClsResource(SinkInfo sinkInfo) {
        ClsDataNodeDTO clsDataNode = getClsDataNode(sinkInfo);
        ClsSinkDTO clsSinkDTO = JsonUtils.parseObject(sinkInfo.getExtParams(), ClsSinkDTO.class);
        try {
            createOrUpdateTopicName(clsDataNode, clsSinkDTO);
            sinkInfo.setExtParams(JsonUtils.toJsonString(clsSinkDTO));
            // create topic index by tokenizer
            clsOperator.createTopicIndex(clsSinkDTO.getTokenizer(), clsSinkDTO.getTopicId(),
                    clsDataNode.getManageSecretId(),
                    clsDataNode.getManageSecretKey(), clsDataNode.getRegion());
            // update set topic id into sink info
            updateSinkInfo(sinkInfo, clsSinkDTO);
            String info = "success to create cls resource";
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_SUCCESSFUL.getCode(), info);
            LOG.info("update cls info status success for sinkId= {}, topicName = {}", sinkInfo.getSinkName(),
                    clsSinkDTO.getTopicName());
        } catch (Exception e) {
            String errMsg = "Create cls topic failed: " + e.getMessage();
            LOG.error(errMsg, e);
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_FAILED.getCode(), errMsg);
            throw new BusinessException(errMsg);
        }
    }

    private void createOrUpdateTopicName(ClsDataNodeDTO clsDataNode, ClsSinkDTO clsSinkDTO)
            throws Exception {
        String topicName = clsSinkDTO.getTopicName();
        if (StringUtils.isBlank(clsSinkDTO.getTopicId())) {
            // if topic don't exist, create topic in cls
            String topicId = clsOperator.createTopicReturnTopicId(clsSinkDTO.getTopicName(), clsDataNode.getLogSetId(),
                    clsSinkDTO.getTag(), clsSinkDTO.getStorageDuration(), clsDataNode.getManageSecretId(),
                    clsDataNode.getManageSecretKey(),
                    clsDataNode.getRegion());
            clsSinkDTO.setTopicId(topicId);
        } else {
            topicName = clsOperator.describeTopicNameByTopicId(clsSinkDTO.getTopicId(), clsDataNode.getLogSetId(),
                    clsDataNode.getManageSecretId(), clsDataNode.getManageSecretKey(),
                    clsDataNode.getRegion());
            if (!Objects.equals(topicName, clsSinkDTO.getTopicName())) {
                clsOperator.modifyTopicNameByTopicId(clsSinkDTO.getTopicId(), clsSinkDTO.getTopicName(),
                        clsDataNode.getManageSecretId(), clsDataNode.getManageSecretKey(),
                        clsDataNode.getRegion());
            }
        }
    }

    private void updateSinkInfo(SinkInfo sinkInfo, ClsSinkDTO clsSinkDTO) {
        StreamSinkEntity streamSinkEntity = streamSinkEntityMapper.selectByPrimaryKey(sinkInfo.getId());
        streamSinkEntity.setExtParams(JsonUtils.toJsonString(clsSinkDTO));
        streamSinkEntityMapper.updateByIdSelective(streamSinkEntity);
    }

    private ClsDataNodeDTO getClsDataNode(SinkInfo sinkInfo) {
        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectByUniqueKey(sinkInfo.getDataNodeName(),
                DataNodeType.CLS);
        return JsonUtils.parseObject(dataNodeEntity.getExtParams(), ClsDataNodeDTO.class);
    }

}
