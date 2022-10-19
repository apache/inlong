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

package org.apache.inlong.manager.plugin.poller;

import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobStatus;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.SortStatus;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.plugin.flink.FlinkService;
import org.apache.inlong.manager.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.workflow.plugin.SortStatusPoller;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class FlinkStatusPoller implements SortStatusPoller {

    @Override
    public Map<String, SortStatus> poll(List<InlongGroupInfo> groupInfos, String credentials) {
        Map<String, SortStatus> statusMap = new HashMap<>();
        for (InlongGroupInfo groupInfo : groupInfos) {
            String groupId = groupInfo.getInlongGroupId();
            try {
                List<InlongGroupExtInfo> extList = groupInfo.getExtList();
                log.debug("inlong group ext info: {}", extList);

                Map<String, String> kvConf = new HashMap<>();
                extList.forEach(groupExtInfo -> kvConf.put(groupExtInfo.getKeyName(), groupExtInfo.getKeyValue()));
                String sortExt = kvConf.get(InlongConstants.SORT_PROPERTIES);
                if (StringUtils.isNotEmpty(sortExt)) {
                    Map<String, String> result = JsonUtils.OBJECT_MAPPER.convertValue(
                            JsonUtils.OBJECT_MAPPER.readTree(sortExt), new TypeReference<Map<String, String>>() {
                            });
                    kvConf.putAll(result);
                }

                String jobId = kvConf.get(InlongConstants.SORT_JOB_ID);
                if (StringUtils.isBlank(jobId)) {
                    statusMap.put(groupId, SortStatus.NOT_EXISTS);
                    continue;
                }

                String sortUrl = kvConf.get(InlongConstants.SORT_URL);
                FlinkService flinkService = new FlinkService(sortUrl);
                SortStatus status = convertToSortStatus(flinkService.getJobStatus(jobId));
                statusMap.put(groupId, status);
            } catch (Exception e) {
                log.error("polling sort status failed for group " + groupId, e);
                statusMap.put(groupId, SortStatus.UNKNOWN);
            }
        }
        return statusMap;
    }

    private SortStatus convertToSortStatus(JobStatus jobStatus) {
        switch (jobStatus) {
            case CREATED:
            case INITIALIZING:
                return SortStatus.NEW;
            case RUNNING:
                return SortStatus.RUNNING;
            case FAILED:
                return SortStatus.FAILED;
            case CANCELED:
                return SortStatus.STOPPED;
            case SUSPENDED:
                return SortStatus.PAUSED;
            case FINISHED:
                return SortStatus.FINISHED;
            case FAILING:
            case CANCELLING:
            case RESTARTING:
            case RECONCILING:
                return SortStatus.OPERATING;
        }
        return SortStatus.UNKNOWN;
    }
}
