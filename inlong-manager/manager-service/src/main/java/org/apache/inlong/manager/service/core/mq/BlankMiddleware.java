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
import org.springframework.stereotype.Component;

/**
 * BlankMiddleware is used when no message queue is needed
 */
@Component
@Slf4j
public class BlankMiddleware implements Middleware {

    @Override
    public MQType type() {
        return MQType.NONE;
    }

    @Override
    public int save(InlongGroupMqExtBase mqExtBaseInfo) {
        log.warn("There is no implementation about this mq type:{}", type());
        return -1;
    }

    @Override
    public InlongGroupMqExtBase get(String groupId) {
        InlongGroupMqExtBase mqExtBase = new InlongGroupMqExtBase();
        mqExtBase.setMiddlewareType(MQType.NONE.getType());
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
        log.error("There is no implementation about this mq type:{}", type());
        return null;
    }

    @Override
    public InlongGroupInfo packSpecificInfo(InlongGroupInfo groupInfo) {
        return groupInfo;
    }

    @Override
    public int delete(String groupId) {
        log.warn("There is no implementation about this mq type:{}", type());
        return -1;
    }
}
