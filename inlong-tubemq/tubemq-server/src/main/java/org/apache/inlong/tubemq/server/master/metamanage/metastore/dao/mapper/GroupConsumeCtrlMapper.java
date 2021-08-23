/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.mapper;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.inlong.tubemq.server.common.utils.ProcessResult;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.GroupConsumeCtrlEntity;



public interface GroupConsumeCtrlMapper extends AbstractMapper {

    boolean addGroupConsumeCtrlConf(GroupConsumeCtrlEntity entity, ProcessResult result);

    boolean updGroupConsumeCtrlConf(GroupConsumeCtrlEntity entity, ProcessResult result);

    boolean delGroupConsumeCtrlConf(String recordKey, ProcessResult result);

    boolean delGroupConsumeCtrlConf(String groupName,
                                    String topicName,
                                    ProcessResult result);

    boolean isTopicNameInUsed(String topicName);

    boolean hasGroupConsumeCtrlConf(String groupName);

    GroupConsumeCtrlEntity getGroupConsumeCtrlConfByRecKey(String recordKey);

    List<GroupConsumeCtrlEntity> getConsumeCtrlByTopicName(String topicName);

    List<GroupConsumeCtrlEntity> getConsumeCtrlByGroupName(String groupName);

    GroupConsumeCtrlEntity getConsumeCtrlByGroupAndTopic(String groupName, String topicName);

    Map<String/* group */, List<GroupConsumeCtrlEntity>> getConsumeCtrlInfoMap(
            Set<String> groupSet, Set<String> topicSet, GroupConsumeCtrlEntity qryEntry);

    List<GroupConsumeCtrlEntity> getGroupConsumeCtrlConf(GroupConsumeCtrlEntity qryEntity);

    Set<String> getMatchedRecords(Set<String> groupSet, Set<String> topicSet);

}
