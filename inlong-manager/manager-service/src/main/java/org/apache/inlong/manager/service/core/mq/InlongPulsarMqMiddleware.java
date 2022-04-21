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

package org.apache.inlong.manager.service.core.mq;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.common.pojo.dataproxy.PulsarClusterInfo;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupMqExtBase;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPulsarInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupTopicResponse;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongGroupPulsarEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupPulsarEntityMapper;
import org.apache.inlong.manager.service.CommonOperateService;
import org.apache.inlong.manager.service.core.InlongStreamService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class InlongPulsarMqMiddleware implements InlongMqMiddleware {

    @Autowired
    private InlongGroupPulsarEntityMapper groupPulsarMapper;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private CommonOperateService commonOperateService;

    @Override
    public MQType type() {
        return MQType.PULSAR;
    }

    @Override
    public int saveMqInfo(InlongGroupMqExtBase mqExtBaseInfo) {
        Preconditions.checkNotEmpty(mqExtBaseInfo.getInlongGroupId(), "InLong group identifier can not be empty.");
        InlongGroupPulsarInfo pulsarInfo = (InlongGroupPulsarInfo) mqExtBaseInfo;
        log.debug("begin to save inlong group MQ information.");
        // Pulsar params must meet: ackQuorum <= writeQuorum <= ensemble
        Integer ackQuorum = pulsarInfo.getAckQuorum();
        Integer writeQuorum = pulsarInfo.getWriteQuorum();

        Preconditions.checkNotNull(ackQuorum, "Pulsar ackQuorum cannot be empty");
        Preconditions.checkNotNull(writeQuorum, "Pulsar writeQuorum cannot be empty");

        if (!(ackQuorum <= writeQuorum)) {
            throw new BusinessException(ErrorCodeEnum.GROUP_SAVE_FAILED,
                    "Pulsar params must meet: ackQuorum <= writeQuorum");
        }
        // The default value of ensemble is writeQuorum
        pulsarInfo.setEnsemble(writeQuorum);

        // Pulsar entity may already exist, such as unsuccessfully deleted, or modify the MQ type to Tube,
        // need to delete and add the Pulsar entity with the same group id
        InlongGroupPulsarEntity pulsarEntity = groupPulsarMapper.selectByGroupId(pulsarInfo.getInlongGroupId());
        if (pulsarEntity == null) {
            pulsarEntity = CommonBeanUtils.copyProperties(pulsarInfo, InlongGroupPulsarEntity::new);
            pulsarEntity.setIsDeleted(0);
            pulsarEntity.setInlongGroupId(pulsarInfo.getInlongGroupId());
            return groupPulsarMapper.insertSelective(pulsarEntity);
        } else {
            Integer id = pulsarEntity.getId();
            pulsarEntity = CommonBeanUtils.copyProperties(pulsarInfo, InlongGroupPulsarEntity::new);
            pulsarEntity.setId(id);
            return groupPulsarMapper.updateByPrimaryKeySelective(pulsarEntity);
        }
    }

    @Override
    public InlongGroupMqExtBase get(String groupId) {
        InlongGroupPulsarEntity pulsarEntity = groupPulsarMapper.selectByGroupId(groupId);
        Preconditions.checkNotNull(pulsarEntity, "Pulsar info not found by the groupId=" + groupId);
        InlongGroupPulsarInfo pulsarInfo = CommonBeanUtils.copyProperties(pulsarEntity,
                InlongGroupPulsarInfo::new);
        pulsarInfo.setMiddlewareType(type().name());
        return pulsarInfo;
    }

    @Override
    public int update(InlongGroupMqExtBase mqExtBaseInfo) {
        InlongGroupPulsarInfo pulsarInfo = (InlongGroupPulsarInfo) mqExtBaseInfo;
        Preconditions.checkNotNull(pulsarInfo, "Pulsar info cannot be empty, as the middleware is Pulsar");
        Preconditions.checkNotEmpty(pulsarInfo.getInlongGroupId(), "inlong group id can not be empty");
        Integer writeQuorum = pulsarInfo.getWriteQuorum();
        Integer ackQuorum = pulsarInfo.getAckQuorum();
        if (!(ackQuorum <= writeQuorum)) {
            throw new BusinessException(ErrorCodeEnum.GROUP_SAVE_FAILED,
                    "Pulsar params must meet: ackQuorum <= writeQuorum");
        }
        InlongGroupPulsarEntity pulsarEntity = CommonBeanUtils.copyProperties(pulsarInfo,
                InlongGroupPulsarEntity::new);
        pulsarEntity.setInlongGroupId(mqExtBaseInfo.getInlongGroupId());
        return groupPulsarMapper.updateByIdentifierSelective(pulsarEntity);
    }

    @Override
    public InlongGroupTopicResponse getTopic(InlongGroupInfo groupInfo) {
        // Pulsar's topic corresponds to the inlong stream one-to-one
        InlongGroupTopicResponse topicVO = new InlongGroupTopicResponse();
        topicVO.setDsTopicList(streamService.getTopicList(groupInfo.getInlongGroupId()));
        topicVO.setPulsarAdminUrl(commonOperateService.getSpecifiedParam(InlongGroupSettings.PULSAR_ADMIN_URL));
        topicVO.setPulsarServiceUrl(commonOperateService.getSpecifiedParam(InlongGroupSettings.PULSAR_SERVICE_URL));
        topicVO.setInlongGroupId(groupInfo.getInlongGroupId());
        topicVO.setMiddlewareType(type().name());
        return topicVO;
    }

    @Override
    public InlongGroupInfo packMqSpecificInfo(InlongGroupInfo groupInfo) {
        PulsarClusterInfo pulsarCluster = commonOperateService.getPulsarClusterInfo(type().name());
        groupInfo.setPulsarAdminUrl(pulsarCluster.getAdminUrl());
        groupInfo.setPulsarServiceUrl(pulsarCluster.getBrokerServiceUrl());
        return groupInfo;
    }

    @Override
    public int delete(String groupId) {
        // To logically delete the associated pulsar table
        return groupPulsarMapper.logicDeleteByGroupId(groupId);
    }
}
