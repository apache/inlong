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

package org.apache.inlong.manager.client.api;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.inlong.manager.client.api.StreamSource.State;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.util.AssertUtil;
import org.apache.inlong.manager.client.api.util.GsonUtil;
import org.apache.inlong.manager.common.enums.GroupState;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class InlongGroupContext implements Serializable {

    private String groupId;

    private String groupName;

    private InlongGroupConf groupConf;

    private Map<String, InlongStream> inlongStreamMap;

    /**
     * Extension configuration for Inlong group.
     */
    private Map<String, String> extensions;

    /**
     * Logs for Inlong group, taskName->logs.
     */
    private Map<String, List<String>> groupLogs;

    /**
     * Error message for Inlong group, taskName->exceptionMsg.
     */
    private Map<String, List<String>> groupErrLogs;

    /**
     * Logs for each stream, key: streamName, value: componentName->log
     */
    private Map<String, Map<String, List<String>>> streamErrLogs = Maps.newHashMap();

    private InlongGroupState state;

    public InlongGroupContext(InnerGroupContext groupContext, InlongGroupConf streamGroupConf) {
        InlongGroupInfo groupInfo = groupContext.getGroupInfo();
        AssertUtil.notNull(groupInfo);
        this.groupId = groupInfo.getInlongGroupId();
        this.groupName = groupInfo.getName();
        this.groupConf = streamGroupConf;
        this.inlongStreamMap = groupContext.getStreamMap();
        this.groupErrLogs = Maps.newHashMap();
        this.groupLogs = Maps.newHashMap();
        this.state = InlongGroupState.parseByBizStatus(groupInfo.getStatus());
        recheckState();
        this.extensions = Maps.newHashMap();
        List<InlongGroupExtInfo> extInfos = groupInfo.getExtList();
        if (CollectionUtils.isNotEmpty(extInfos)) {
            extInfos.stream().forEach(extInfo -> {
                extensions.put(extInfo.getKeyName(), extInfo.getKeyValue());
            });
        }
    }

    private void recheckState() {
        if (MapUtils.isEmpty(this.inlongStreamMap)) {
            return;
        }
        List<StreamSource> failedSources = Lists.newArrayList();
        this.inlongStreamMap.values().stream().forEach(inlongStream -> {
            Map<String, StreamSource> sources = inlongStream.getSources();
            if (MapUtils.isNotEmpty(sources)) {
                for (Map.Entry<String, StreamSource> entry : sources.entrySet()) {
                    StreamSource source = entry.getValue();
                    if (source.getState() == State.FAILED) {
                        failedSources.add(source);
                    }
                }
            }
        });
        if (CollectionUtils.isNotEmpty(failedSources)) {
            this.state = InlongGroupState.FAILED;
            for (StreamSource failedSource : failedSources) {
                this.groupErrLogs.computeIfAbsent("failedSources", Lists::newArrayList)
                        .add(GsonUtil.toJson(failedSource));
            }
        }
    }

    public enum InlongGroupState {
        CREATE, REJECTED, INITIALIZING, OPERATING, STARTED, FAILED, STOPPED, FINISHED, DELETED;

        // Reference to  org.apache.inlong.manager.common.enums.GroupState code
        public static InlongGroupState parseByBizStatus(int bizCode) {

            GroupState groupState = GroupState.forCode(bizCode);

            switch (groupState) {
                case DRAFT:
                case TO_BE_SUBMIT:
                    return CREATE;
                case DELETING:
                case SUSPENDING:
                case RESTARTING:
                    return OPERATING;
                case APPROVE_REJECTED:
                    return REJECTED;
                case TO_BE_APPROVAL:
                case APPROVE_PASSED:
                case CONFIG_ING:
                    return INITIALIZING;
                case CONFIG_FAILED:
                    return FAILED;
                case CONFIG_SUCCESSFUL:
                case RESTARTED:
                    return STARTED;
                case SUSPENDED:
                    return STOPPED;
                case FINISH:
                    return FINISHED;
                case DELETED:
                    return DELETED;
                default:
                    throw new IllegalArgumentException(String.format("Unsupported status %s for group", bizCode));
            }
        }
    }

}
