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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.inlong.manager.client.api.inner.InnerGroupContext;
import org.apache.inlong.manager.client.api.util.GsonUtils;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.common.pojo.stream.StreamSource;
import org.apache.inlong.manager.common.pojo.stream.StreamSource.State;
import org.apache.inlong.manager.common.util.AssertUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Inlong group context.
 */
@Data
@Slf4j
public class InlongGroupContext implements Serializable {

    private String groupId;

    private String groupName;

    private InlongGroupInfo groupInfo;

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

    private InlongGroupStatus status;

    public InlongGroupContext(InnerGroupContext groupContext) {
        InlongGroupInfo groupInfo = groupContext.getGroupInfo();
        AssertUtils.notNull(groupInfo);
        this.groupId = groupInfo.getInlongGroupId();
        this.groupName = groupInfo.getName();
        this.groupInfo = groupInfo;
        this.inlongStreamMap = groupContext.getStreamMap();
        this.groupErrLogs = Maps.newHashMap();
        this.groupLogs = Maps.newHashMap();
        this.status = InlongGroupStatus.parseStatusByCode(groupInfo.getStatus());
        recheckState();
        this.extensions = Maps.newHashMap();
        List<InlongGroupExtInfo> extInfos = groupInfo.getExtList();
        if (CollectionUtils.isNotEmpty(extInfos)) {
            extInfos.forEach(extInfo -> {
                extensions.put(extInfo.getKeyName(), extInfo.getKeyValue());
            });
        }
    }

    private void recheckState() {
        if (MapUtils.isEmpty(this.inlongStreamMap)) {
            return;
        }
        List<StreamSource> sourcesInGroup = Lists.newArrayList();
        List<StreamSource> failedSources = Lists.newArrayList();
        this.inlongStreamMap.values().forEach(inlongStream -> {
            Map<String, StreamSource> sources = inlongStream.getSources();
            if (MapUtils.isNotEmpty(sources)) {
                for (Map.Entry<String, StreamSource> entry : sources.entrySet()) {
                    StreamSource source = entry.getValue();
                    if (source != null) {
                        sourcesInGroup.add(source);
                        if (source.getState() == State.FAILED) {
                            failedSources.add(source);
                        }
                    }
                }
            }
        });
        // check if any stream source is failed
        if (CollectionUtils.isNotEmpty(failedSources)) {
            this.status = InlongGroupStatus.FAILED;
            for (StreamSource failedSource : failedSources) {
                this.groupErrLogs.computeIfAbsent("failedSources", Lists::newArrayList)
                        .add(GsonUtils.toJson(failedSource));
            }
            return;
        }
        // check if any stream source is in indirect state
        switch (this.status) {
            case STARTED:
                for (StreamSource source : sourcesInGroup) {
                    if (source.getState() != State.NORMAL) {
                        log.warn("StreamSource:{} is not started", source);
                        this.status = InlongGroupStatus.INITIALIZING;
                        break;
                    }
                }
                return;
            case STOPPED:
                for (StreamSource source : sourcesInGroup) {
                    if (source.getState() != State.FROZEN) {
                        log.warn("StreamSource:{} is not stopped", source);
                        this.status = InlongGroupStatus.OPERATING;
                        break;
                    }
                }
                return;
            default:
        }
    }

    public enum InlongGroupStatus {

        CREATE, REJECTED, INITIALIZING, OPERATING, STARTED, FAILED, STOPPED, FINISHED, DELETED;

        /**
         * Parse InlongGroupStatus from the status code
         *
         * @param code of status
         * @see org.apache.inlong.manager.common.enums.GroupStatus
         */
        public static InlongGroupStatus parseStatusByCode(int code) {
            GroupStatus groupStatus = GroupStatus.forCode(code);
            switch (groupStatus) {
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
                    throw new IllegalArgumentException(String.format("Unsupported status %s for group", code));
            }
        }

        /**
         * Parse group status code by the status string
         *
         * @see org.apache.inlong.manager.common.enums.GroupStatus
         */
        public static List<Integer> parseStatusCodeByStr(String status) {
            InlongGroupStatus groupStatus;
            try {
                groupStatus = InlongGroupStatus.valueOf(status);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("Unsupported status %s for group", status));
            }

            List<Integer> statusList = new ArrayList<>();
            switch (groupStatus) {
                case CREATE:
                    statusList.add(GroupStatus.DRAFT.getCode());
                    return statusList;
                case OPERATING:
                    statusList.add(GroupStatus.DELETING.getCode());
                    statusList.add(GroupStatus.SUSPENDING.getCode());
                    statusList.add(GroupStatus.RESTARTING.getCode());
                    return statusList;
                case REJECTED:
                    statusList.add(GroupStatus.APPROVE_REJECTED.getCode());
                    return statusList;
                case INITIALIZING:
                    statusList.add(GroupStatus.TO_BE_APPROVAL.getCode());
                    statusList.add(GroupStatus.APPROVE_PASSED.getCode());
                    statusList.add(GroupStatus.CONFIG_ING.getCode());
                    return statusList;
                case FAILED:
                    statusList.add(GroupStatus.CONFIG_FAILED.getCode());
                    return statusList;
                case STARTED:
                    statusList.add(GroupStatus.RESTARTED.getCode());
                    statusList.add(GroupStatus.CONFIG_SUCCESSFUL.getCode());
                    return statusList;
                case STOPPED:
                    statusList.add(GroupStatus.SUSPENDED.getCode());
                    return statusList;
                case FINISHED:
                    statusList.add(GroupStatus.FINISH.getCode());
                    return statusList;
                case DELETED:
                    statusList.add(GroupStatus.DELETED.getCode());
                    return statusList;
                default:
                    throw new IllegalArgumentException(String.format("Unsupported status %s for group", status));
            }
        }
    }

}
