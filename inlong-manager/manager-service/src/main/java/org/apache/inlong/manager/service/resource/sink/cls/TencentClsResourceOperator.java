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
import org.apache.inlong.manager.common.exceptions.WorkflowException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.dao.entity.DataNodeEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.DataNodeEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.node.cls.TencentClsDataNodeDTO;
import org.apache.inlong.manager.pojo.sink.SinkInfo;
import org.apache.inlong.manager.pojo.sink.cls.TencentClsSinkDTO;
import org.apache.inlong.manager.service.resource.sink.SinkResourceOperator;
import org.apache.inlong.manager.service.sink.StreamSinkService;

import com.tencentcloudapi.cls.v20201016.ClsClient;
import com.tencentcloudapi.cls.v20201016.models.CreateTopicRequest;
import com.tencentcloudapi.cls.v20201016.models.CreateTopicResponse;
import com.tencentcloudapi.cls.v20201016.models.Tag;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.exception.TencentCloudSDKException;
import com.tencentcloudapi.common.profile.ClientProfile;
import com.tencentcloudapi.common.profile.HttpProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class TencentClsResourceOperator implements SinkResourceOperator {

    private static final Logger LOG = LoggerFactory.getLogger(TencentClsResourceOperator.class);

    @Autowired
    private DataNodeEntityMapper dataNodeEntityMapper;
    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private StreamSinkEntityMapper streamSinkEntityMapper;

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
        this.createTopicID(sinkInfo);
    }

    private void createTopicID(SinkInfo sinkInfo) {
        TencentClsDataNodeDTO tencentClsDataNode = getTencentClsDataNode(sinkInfo);
        TencentClsSinkDTO tencentClsSinkDTO = JsonUtils.parseObject(sinkInfo.getExtParams(), TencentClsSinkDTO.class);
        try {
            Credential cred = new Credential(tencentClsDataNode.getManageSecretId(),
                    tencentClsDataNode.getManageSecretId());
            HttpProfile httpProfile = new HttpProfile();
            httpProfile.setEndpoint(tencentClsDataNode.getEndpoint());
            ClientProfile clientProfile = new ClientProfile();
            clientProfile.setHttpProfile(httpProfile);
            ClsClient client = new ClsClient(cred, tencentClsDataNode.getRegion(), clientProfile);
            CreateTopicRequest req = new CreateTopicRequest();
            String allTag = tencentClsSinkDTO.getTag();
            String[] allTags = allTag.split("\\|");
            Tag[] tags = convertTags(allTags);
            req.setTags(tags);
            req.setLogsetId(tencentClsDataNode.getLogSetID());
            req.setTopicName(tencentClsSinkDTO.getTopicName());
            CreateTopicResponse resp = client.CreateTopic(req);
            LOG.info("create cls topic {} success ,topicId {}", tencentClsSinkDTO.getTopicName(), resp.getTopicId());
            tencentClsSinkDTO.setTopicID(resp.getTopicId());
            sinkInfo.setExtParams(JsonUtils.toJsonString(tencentClsSinkDTO));
            StreamSinkEntity streamSinkEntity = new StreamSinkEntity();
            CommonBeanUtils.copyProperties(sinkInfo, streamSinkEntity, true);
            streamSinkEntityMapper.updateByIdSelective(streamSinkEntity);
            String info = "success to create cls resource";
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_SUCCESSFUL.getCode(), info);
            LOG.info("update cls sink info status {} success ,topicId {}", tencentClsSinkDTO.getTopicName(),
                    streamSinkEntity.getExtParams());
        } catch (TencentCloudSDKException e) {
            String errMsg = "Create cls topic  failed: " + e.getMessage();
            LOG.error(errMsg, e);
            sinkService.updateStatus(sinkInfo.getId(), SinkStatus.CONFIG_FAILED.getCode(), errMsg);
            throw new WorkflowException(errMsg);
        }
    }

    private Tag[] convertTags(String[] allTags) {
        Tag[] tags = new Tag[allTags.length];
        for (int i = 0; i < allTags.length; i++) {
            String tag = allTags[i];
            String[] keyAndValueOfTag = tag.split("=");
            Tag tagInfo = new Tag();
            tagInfo.set(keyAndValueOfTag[0], keyAndValueOfTag[1]);
            tags[i] = tagInfo;
        }
        return tags;
    }

    private TencentClsDataNodeDTO getTencentClsDataNode(SinkInfo sinkInfo) {
        DataNodeEntity dataNodeEntity = dataNodeEntityMapper.selectByUniqueKey(sinkInfo.getDataNodeName(),
                DataNodeType.CLS);
        return JsonUtils.parseObject(dataNodeEntity.getExtParams(), TencentClsDataNodeDTO.class);
    }

}
