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

package org.apache.inlong.agent.plugin.task.logcollection.cos;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.plugin.task.logcollection.LogAbstractTask;
import org.apache.inlong.agent.plugin.task.logcollection.cos.FileScanner.BasicFileInfo;
import org.apache.inlong.agent.plugin.utils.cos.COSUtils;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DateTransUtils;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Watch directory, if new valid files are created, create instance correspondingly.
 */
public class COSTask extends LogAbstractTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(COSTask.class);
    private String originPattern;
    private long lastScanTime = 0;
    public final long SCAN_INTERVAL = 1 * 60 * 1000;
    private COSClient cosClient;
    private String bucketName;
    private String secretId;
    private String secretKey;
    private String strRegion;

    @Override
    protected int getInstanceLimit() {
        return taskProfile.getInt(TaskConstants.COS_MAX_NUM);
    }

    @Override
    protected void initTask() {
        super.initTask();
        timeOffset = taskProfile.get(TaskConstants.TASK_COS_TIME_OFFSET, "");
        retry = taskProfile.getBoolean(TaskConstants.COS_TASK_RETRY, false);
        originPattern = taskProfile.get(TaskConstants.COS_TASK_PATTERN);
        bucketName = taskProfile.get(TaskConstants.COS_TASK_BUCKET_NAME);
        secretId = taskProfile.get(TaskConstants.COS_TASK_SECRET_ID);
        secretKey = taskProfile.get(TaskConstants.COS_TASK_SECRET_KEY);
        strRegion = taskProfile.get(TaskConstants.COS_TASK_REGION);
        cosClient = COSUtils.createCli(secretId, secretKey, strRegion);
        if (retry) {
            initRetryTask(taskProfile);
        }
    }

    private boolean initRetryTask(TaskProfile profile) {
        String dataTimeFrom = profile.get(TaskConstants.COS_TASK_TIME_FROM, "");
        String dataTimeTo = profile.get(TaskConstants.COS_TASK_TIME_TO, "");
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
        if (!profile.hasKey(TaskConstants.COS_TASK_CYCLE_UNIT)) {
            LOGGER.error("task profile needs cos cycle unit");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_CYCLE_UNIT)) {
            LOGGER.error("task profile needs cycle unit");
            return false;
        }
        if (profile.get(TaskConstants.TASK_CYCLE_UNIT)
                .compareTo(profile.get(TaskConstants.COS_TASK_CYCLE_UNIT)) != 0) {
            LOGGER.error("task profile cycle unit must be consistent");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_TIME_ZONE)) {
            LOGGER.error("task profile needs time zone");
            return false;
        }
        boolean ret = profile.hasKey(TaskConstants.COS_TASK_PATTERN)
                && profile.hasKey(TaskConstants.COS_MAX_NUM);
        if (!ret) {
            LOGGER.error("task profile needs file keys");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_COS_TIME_OFFSET)) {
            LOGGER.error("task profile needs time offset");
            return false;
        }
        if (profile.getBoolean(TaskConstants.COS_TASK_RETRY, false)) {
            if (!initRetryTask(profile)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected void releaseTask() {
        cosClient.shutdown();
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
        List<BasicFileInfo> fileInfos = FileScanner.scanTaskBetweenTimes(cosClient, bucketName, originPattern,
                taskProfile.getCycleUnit(), timeOffset, startTime, endTime, retry);
        LOGGER.info("taskId {} scan {} get file count {}", getTaskId(), originPattern, fileInfos.size());
        fileInfos.forEach((fileInfo) -> {
            addToEvenMap(fileInfo.fileName, fileInfo.dataTime);
            if (retry) {
                instanceCount++;
            }
        });
    }

    private boolean isInEventMap(String fileName, String dataTime) {
        Map<String, InstanceProfile> fileToProfile = eventMap.get(dataTime);
        if (fileToProfile == null) {
            return false;
        }
        return fileToProfile.get(fileName) != null;
    }

    @Override
    protected void dealWithEventMap() {
        removeTimeoutEvent(eventMap, retry);
        dealWithEventMapWithCycle();
    }

    private void addToEvenMap(String fileName, String dataTime) {
        if (isInEventMap(fileName, dataTime)) {
            LOGGER.info("add to evenMap isInEventMap returns true skip taskId {} dataTime {} fileName {}",
                    taskProfile.getTaskId(), dataTime, fileName);
            return;
        }
        ObjectMetadata meta = cosClient.getObjectMetadata(bucketName, fileName);
        Long fileUpdateTime = meta.getLastModified().getTime();
        if (!shouldAddAgain(fileName, fileUpdateTime)) {
            LOGGER.info("add to evenMap shouldAddAgain returns false skip taskId {} dataTime {} fileName {}",
                    taskProfile.getTaskId(), dataTime, fileName);
            return;
        }
        Map<String, InstanceProfile> sameDataTimeEvents = eventMap.computeIfAbsent(dataTime,
                mapKey -> new ConcurrentHashMap<>());
        boolean containsInMemory = sameDataTimeEvents.containsKey(fileName);
        if (containsInMemory) {
            LOGGER.error("should not happen! may be {} has been deleted and add again", fileName);
            return;
        }
        String cycleUnit = taskProfile.getCycleUnit();
        InstanceProfile instanceProfile = taskProfile.createInstanceProfile(fileName, cycleUnit, dataTime,
                fileUpdateTime);
        sameDataTimeEvents.put(fileName, instanceProfile);
        LOGGER.info("add to eventMap taskId {} dataTime {} fileName {}", taskProfile.getTaskId(), dataTime, fileName);
    }
}
