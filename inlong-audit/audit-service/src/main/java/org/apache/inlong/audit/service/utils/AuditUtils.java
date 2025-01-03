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

package org.apache.inlong.audit.service.utils;

import org.apache.inlong.audit.service.config.ConfigConstants;
import org.apache.inlong.audit.service.entities.AuditCycle;
import org.apache.inlong.audit.service.entities.StatData;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;

public class AuditUtils {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(ConfigConstants.DATE_FORMAT);

    public static double calculateDiffRatio(long srcCount, long destCount) {
        if (srcCount == 0 && destCount == 0) {
            return 0;
        } else if (srcCount == 0) {
            return 1;
        } else {
            return Math.abs((double) (srcCount - destCount) / srcCount);
        }
    }

    public static StatData getMaxAuditVersionAuditData(List<StatData> statDataListData) {
        StatData maxAuditVersionStatData = null;
        for (StatData statData : statDataListData) {
            if (maxAuditVersionStatData == null
                    || statData.getAuditVersion() > maxAuditVersionStatData.getAuditVersion()) {
                maxAuditVersionStatData = statData;
            }
        }
        return maxAuditVersionStatData;
    }

    public static AuditCycle getAuditCycleTime(String startTime, String endTime) {
        LocalDateTime startDateTime = LocalDateTime.parse(startTime, dateTimeFormatter);
        LocalDateTime endDateTime = LocalDateTime.parse(endTime, dateTimeFormatter);
        return AuditCycle.fromInt((int) ChronoUnit.MINUTES.between(startDateTime, endDateTime));
    }

    public static StatData mergeStatDataList(List<StatData> statDataList) {
        if (statDataList == null || statDataList.isEmpty()) {
            return null;
        }

        // Assuming all other fields are the same, we take the first element as the base
        StatData base = statDataList.get(0);

        // Summing up the count
        long totalCount = 0L;
        for (StatData statData : statDataList) {
            if (statData.getCount() != null) {
                totalCount += statData.getCount();
            }
        }

        // Creating a new StatData object with the summed count
        StatData mergedStatData = new StatData();
        mergedStatData.setAuditVersion(base.getAuditVersion());
        mergedStatData.setLogTs(base.getLogTs());
        mergedStatData.setInlongGroupId(base.getInlongGroupId());
        mergedStatData.setInlongStreamId(base.getInlongStreamId());
        mergedStatData.setAuditId(base.getAuditId());
        mergedStatData.setAuditTag(base.getAuditTag());
        mergedStatData.setCount(totalCount);
        mergedStatData.setSize(base.getSize());
        mergedStatData.setDelay(base.getDelay());
        mergedStatData.setUpdateTime(base.getUpdateTime());
        mergedStatData.setIp(base.getIp());
        mergedStatData.setSourceName(base.getSourceName());

        return mergedStatData;
    }
}
