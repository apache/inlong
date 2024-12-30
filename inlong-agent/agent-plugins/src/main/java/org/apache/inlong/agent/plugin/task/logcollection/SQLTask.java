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

package org.apache.inlong.agent.plugin.task.logcollection;

import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.plugin.utils.regex.Scanner;
import org.apache.inlong.agent.plugin.utils.regex.Scanner.FinalPatternInfo;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DateTransUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.List;

public class SQLTask extends LogAbstractTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLTask.class);
    private String originPattern;
    private long lastScanTime = 0;
    public final long SCAN_INTERVAL = 1 * 60 * 1000;

    @Override
    protected int getInstanceLimit() {
        return taskProfile.getInt(TaskConstants.SQL_MAX_NUM);
    }

    @Override
    protected void initTask() {
        super.initTask();
        timeOffset = taskProfile.get(TaskConstants.SQL_TIME_OFFSET, "");
        retry = taskProfile.getBoolean(TaskConstants.SQL_TASK_RETRY, false);
        originPattern = taskProfile.get(TaskConstants.SQL_TASK_SQL);
        if (retry) {
            initRetryTask(taskProfile);
        }
    }

    private boolean initRetryTask(TaskProfile profile) {
        String dataTimeFrom = profile.get(TaskConstants.SQL_TASK_TIME_FROM, "");
        String dataTimeTo = profile.get(TaskConstants.SQL_TASK_TIME_TO, "");
        try {
            startTime = DateTransUtils.timeStrConvertToMillSec(dataTimeFrom, profile.getCycleUnit());
            endTime = DateTransUtils.timeStrConvertToMillSec(dataTimeTo, profile.getCycleUnit());
        } catch (ParseException e) {
            LOGGER.error("retry task time error start {} end {}", dataTimeFrom, dataTimeTo, e);
            return false;
        }
        return true;
    }

    @Override
    public boolean isProfileValid(TaskProfile profile) {
        if (!profile.allRequiredKeyExist()) {
            LOGGER.error("task profile needs all required key");
            return false;
        }
        if (!profile.hasKey(TaskConstants.SQL_TASK_CYCLE_UNIT)) {
            LOGGER.error("task profile needs sql cycle unit");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_CYCLE_UNIT)) {
            LOGGER.error("task profile needs cycle unit");
            return false;
        }
        if (profile.get(TaskConstants.TASK_CYCLE_UNIT)
                .compareTo(profile.get(TaskConstants.SQL_TASK_CYCLE_UNIT)) != 0) {
            LOGGER.error("task profile cycle unit must be consistent");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_TIME_ZONE)) {
            LOGGER.error("task profile needs time zone");
            return false;
        }
        boolean ret = profile.hasKey(TaskConstants.SQL_TASK_SQL)
                && profile.hasKey(TaskConstants.SQL_MAX_NUM);
        if (!ret) {
            LOGGER.error("task profile needs file keys");
            return false;
        }
        if (!profile.hasKey(TaskConstants.SQL_TIME_OFFSET)) {
            LOGGER.error("task profile needs time offset");
            return false;
        }
        if (profile.getBoolean(TaskConstants.SQL_TASK_RETRY, false)) {
            if (!initRetryTask(profile)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void releaseTask() {
    }

    @Override
    protected void runForNormal() {
        if (AgentUtils.getCurrentTime() - lastScanTime > SCAN_INTERVAL) {
            scanExistingFile();
            lastScanTime = AgentUtils.getCurrentTime();
        }
        dealWithEventMap();
    }

    @Override
    protected void scanExistingFile() {
        List<FinalPatternInfo> finalPatternInfos = Scanner.getFinalPatternInfos(originPattern,
                taskProfile.getCycleUnit(), timeOffset, startTime, endTime, retry);
        LOGGER.info("taskId {} scan {} get file count {}", getTaskId(), originPattern, finalPatternInfos.size());
        finalPatternInfos.forEach((fileInfo) -> {
            String dataTime = DateTransUtils.millSecConvertToTimeStr(fileInfo.dataTime, taskProfile.getCycleUnit());
            addToEvenMap(fileInfo.finalPattern, dataTime, 0L, taskProfile.getCycleUnit());
            if (retry) {
                instanceCount++;
            }
        });
    }

    @Override
    protected void dealWithEventMap() {
        removeTimeoutEvent(eventMap, retry);
        dealWithEventMapWithCycle();
    }
}
