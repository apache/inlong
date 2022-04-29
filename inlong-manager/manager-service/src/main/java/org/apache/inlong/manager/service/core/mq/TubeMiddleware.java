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
import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupMqExtBase;
import org.apache.inlong.manager.common.pojo.group.InlongGroupTopicResponse;
import org.apache.inlong.manager.common.settings.InlongGroupSettings;
import org.apache.inlong.manager.service.CommonOperateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * TUBE MQ middleware implementation.
 */
@Component
@Slf4j
public class TubeMiddleware implements Middleware {

    @Autowired
    private CommonOperateService commonOperateService;

    @Override
    public MQType type() {
        return MQType.TUBE;
    }

    @Override
    public int save(InlongGroupMqExtBase mqExtBaseInfo) {
        log.warn("There is no implementation about this mq type:{}", type());
        return -1;
    }

    @Override
    public InlongGroupMqExtBase get(String groupId) {
        InlongGroupMqExtBase mqExtBase = new InlongGroupMqExtBase();
        mqExtBase.setMiddlewareType(MQType.TUBE.getType());
        mqExtBase.setInlongGroupId(groupId);
        return mqExtBase;
    }

    @Override
    public int update(InlongGroupMqExtBase mqExtBaseInfo) {
        log.warn("There is no implementation about this mq type:{}", type());
        return -1;
    }

    @Override
    public InlongGroupTopicResponse getTopic(InlongGroupInfo groupInfo) {
        InlongGroupTopicResponse topicVO = new InlongGroupTopicResponse();
        topicVO.setMqResourceObj(groupInfo.getMqResourceObj());
        topicVO.setTubeMasterUrl(commonOperateService.getSpecifiedParam(InlongGroupSettings.TUBE_MASTER_URL));
        topicVO.setInlongGroupId(groupInfo.getInlongGroupId());
        topicVO.setMiddlewareType(type().name());
        return topicVO;
    }

    @Override
    public InlongGroupInfo packSpecificInfo(InlongGroupInfo groupInfo) {
        log.warn("begin to packing specific information about tube mq middleware.");
        groupInfo.setTubeMaster(commonOperateService.getSpecifiedParam(InlongGroupSettings.TUBE_MASTER_URL));
        return groupInfo;
    }

    @Override
    public int delete(String groupId) {
        log.warn("There is no implementation about this mq type:{}", type());
        return -1;
    }

}
