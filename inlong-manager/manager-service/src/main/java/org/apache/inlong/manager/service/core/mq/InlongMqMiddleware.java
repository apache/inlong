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

import org.apache.inlong.manager.common.enums.MQType;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupMqExtBase;
import org.apache.inlong.manager.common.pojo.group.InlongGroupTopicResponse;

public interface InlongMqMiddleware {

    /**
     * Geting inlong group mq type.
     *
     * @return mq type
     */
    MQType type();

    /**
     * Saving inlong group mq info.
     *
     * @return groupId
     */
    int saveMqInfo(InlongGroupMqExtBase mqExtBaseInfo);

    /**
     * Getting MQ information by group identifier.
     *
     * @param groupId inlong-group id
     * @return MQ detail.
     */
    InlongGroupMqExtBase get(String groupId);

    /**
     * Updating mq info.
     *
     * @param mqExtBaseInfo mqExt info
     */
    int update(InlongGroupMqExtBase mqExtBaseInfo);

    /**
     * Getting topic about MQ.
     *
     * @param groupInfo group id
     * @return Topic Value Object
     */
    InlongGroupTopicResponse getTopic(InlongGroupInfo groupInfo);

    /**
     * Packing some specific information to InlongGroupInfo for a certain MQ middleware.
     *
     * @param groupInfo group info
     * @return result group info.
     */
    InlongGroupInfo packMqSpecificInfo(InlongGroupInfo groupInfo);

    /**
     * Deleting middleware info
     *
     * @param groupId group id
     * @return delete result;
     */
    int delete(String groupId);

}
