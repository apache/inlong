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

package org.apache.inlong.manager.service.consume;

import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.dao.entity.InlongConsumeEntity;
import org.apache.inlong.manager.pojo.consumption.InlongConsumeInfo;
import org.apache.inlong.manager.pojo.consumption.InlongConsumeRequest;
import org.apache.inlong.manager.pojo.consumption.pulsar.ConsumePulsarInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupTopicInfo;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ConsumeTubeOperator extends AbstractConsumeOperator {

    @Autowired
    private InlongGroupService groupService;

    @Override
    public String getMQType() {
        return MQType.TUBEMQ;
    }

    @Override
    public Boolean accept(String mqType) {
        return getMQType().equals(mqType);
    }

    @Override
    public void setTopicInfo(InlongConsumeRequest consumeRequest) {
        // Determine whether the consumed topic belongs to this groupId or the inlong stream under it
        String groupId = consumeRequest.getInlongGroupId();
        InlongGroupTopicInfo topicVO = groupService.getTopic(groupId);
        Preconditions.checkNotNull(topicVO, "inlong group not exist: " + groupId);

        // Tube’s topic is the inlong group level, one inlong group, one TubeMQ topic
        String mqResource = topicVO.getMqResource();
        Preconditions.checkTrue(mqResource == null || mqResource.equals(consumeRequest.getTopic()),
                "topic [" + consumeRequest.getTopic() + "] not belong to inlong group " + groupId);

        consumeRequest.setMqType(topicVO.getMqType());
    }

    @Override
    public InlongConsumeInfo getFromEntity(InlongConsumeEntity entity) {
        if (entity == null) {
            throw new BusinessException(ErrorCodeEnum.CONSUMER_NOR_FOUND);
        }

        ConsumePulsarInfo consumeInfo = new ConsumePulsarInfo();
        CommonBeanUtils.copyProperties(entity, consumeInfo);
        return consumeInfo;
    }

    @Override
    protected void setExtParam(InlongConsumeRequest consumeRequest, InlongConsumeEntity entity) {

    }

    @Override
    protected void updateExtParam(InlongConsumeRequest consumeRequest, InlongConsumeEntity exists, String operator) {

    }

}
