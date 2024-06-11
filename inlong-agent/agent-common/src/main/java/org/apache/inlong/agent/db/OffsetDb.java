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

package org.apache.inlong.agent.db;

import org.apache.inlong.agent.conf.OffsetProfile;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.utils.AgentUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * db interface for task profile.
 */
public class OffsetDb {

    private static final Logger LOGGER = LoggerFactory.getLogger(OffsetDb.class);
    private final OffsetStore offsetStore;

    public OffsetDb(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    public List<OffsetProfile> listAllOffsets() {
        List<KeyValueEntity> result = this.offsetStore.findAll(offsetStore.getUniqueKey());
        List<OffsetProfile> offsetList = new ArrayList<>();
        for (KeyValueEntity entity : result) {
            offsetList.add(entity.getAsOffsetProfile());
        }
        return offsetList;
    }

    public OffsetProfile getOffset(String taskId, String instanceId) {
        KeyValueEntity result = offsetStore.get(getKey(taskId, instanceId));
        if (result == null) {
            return null;
        }
        return result.getAsOffsetProfile();
    }

    public void deleteOffset(String taskId, String instanceId) {
        offsetStore.remove(getKey(taskId, instanceId));
    }

    public void setOffset(OffsetProfile offsetProfile) {
        offsetProfile.setLastUpdateTime(AgentUtils.getCurrentTime());
        if (offsetProfile.allRequiredKeyExist()) {
            String keyName = getKey(offsetProfile.getTaskId(),
                    offsetProfile.getInstanceId());
            KeyValueEntity entity = new KeyValueEntity(keyName,
                    offsetProfile.toJsonStr(), offsetProfile.get(TaskConstants.INSTANCE_ID));
            offsetStore.put(entity);
        }
    }

    public String getKey(String taskId, String instanceId) {
        if (offsetStore.getUniqueKey().isEmpty()) {
            return CommonConstants.OFFSET_ID_PREFIX + offsetStore.getSplitter() + taskId
                    + offsetStore.getSplitter() + offsetStore.replaceKeywords(instanceId);
        } else {
            return offsetStore.getUniqueKey() + offsetStore.getSplitter() + CommonConstants.OFFSET_ID_PREFIX
                    + offsetStore.getSplitter() + taskId
                    + offsetStore.getSplitter() + offsetStore.replaceKeywords(instanceId);
        }
    }
}
