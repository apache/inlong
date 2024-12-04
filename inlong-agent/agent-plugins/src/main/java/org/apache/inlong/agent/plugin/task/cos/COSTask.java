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

package org.apache.inlong.agent.plugin.task.cos;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.core.task.TaskAction;
import org.apache.inlong.agent.plugin.task.AbstractTask;
import org.apache.inlong.agent.plugin.task.cos.FileScanner.BasicFileInfo;
import org.apache.inlong.agent.plugin.utils.cos.COSUtils;
import org.apache.inlong.agent.plugin.utils.regex.NewDateUtils;
import org.apache.inlong.agent.plugin.utils.regex.Scanner;
import org.apache.inlong.agent.state.State;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DateTransUtils;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Watch directory, if new valid files are created, create instance correspondingly.
 */
public class COSTask extends AbstractTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(COSTask.class);
    public static final String DEFAULT_COS_INSTANCE = "org.apache.inlong.agent.plugin.instance.COSInstance";
    private static final int INSTANCE_QUEUE_CAPACITY = 10;
    private final Map<String/* dataTime */, Map<String/* fileName */, InstanceProfile>> eventMap =
            new ConcurrentHashMap<>();
    public static final long DAY_TIMEOUT_INTERVAL = 2 * 24 * 3600 * 1000;
    public static final int CORE_THREAD_MAX_GAP_TIME_MS = 60 * 1000;
    private boolean retry;
    private long startTime;
    private long endTime;
    private String originPattern;
    private long lastScanTime = 0;
    public final long SCAN_INTERVAL = 1 * 60 * 1000;
    private volatile boolean runAtLeastOneTime = false;
    private volatile long coreThreadUpdateTime = 0;
    private BlockingQueue<InstanceProfile> instanceQueue;
    private COSClient cosClient;
    private String bucketName;
    private String secretId;
    private String secretKey;
    private String strRegion;
    private String timeOffset = "";

    @Override
    protected int getInstanceLimit() {
        return taskProfile.getInt(TaskConstants.COS_MAX_NUM);
    }

    @Override
    protected void initTask() {
        timeOffset = taskProfile.get(TaskConstants.TASK_COS_TIME_OFFSET, "");
        instanceQueue = new LinkedBlockingQueue<>(INSTANCE_QUEUE_CAPACITY);
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
    protected List<InstanceProfile> getNewInstanceList() {
        if (retry) {
            runForRetry();
        } else {
            runForNormal();
        }
        List<InstanceProfile> list = new ArrayList<>();
        while (list.size() < INSTANCE_QUEUE_CAPACITY && !instanceQueue.isEmpty()) {
            InstanceProfile profile = instanceQueue.poll();
            if (profile != null) {
                list.add(profile);
            }
        }
        return list;
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

    private void runForRetry() {
        if (!runAtLeastOneTime) {
            scanExistingFile();
            runAtLeastOneTime = true;
        }
        dealWithEventMap();
        if (allInstanceFinished()) {
            LOGGER.info("retry task finished, send action to task manager, taskId {}", getTaskId());
            TaskAction action = new TaskAction(org.apache.inlong.agent.core.task.ActionType.FINISH, taskProfile);
            taskManager.submitAction(action);
            doChangeState(State.SUCCEEDED);
        }
    }

    private void runForNormal() {
        if (AgentUtils.getCurrentTime() - lastScanTime > SCAN_INTERVAL) {
            scanExistingFile();
            lastScanTime = AgentUtils.getCurrentTime();
        }
        dealWithEventMap();
    }

    private void scanExistingFile() {
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

    private void dealWithEventMap() {
        removeTimeoutEvent(eventMap, retry);
        dealWithEventMapWithCycle();
    }

    private void dealWithEventMapWithCycle() {
        long startScanTime = startTime;
        long endScanTime = endTime;
        List<String> dataTimeList = Scanner.getDataTimeList(startScanTime, endScanTime, taskProfile.getCycleUnit(),
                timeOffset, retry);
        if (dataTimeList.isEmpty()) {
            LOGGER.error("getDataTimeList get empty list");
            return;
        }
        Set<String> dealtDataTime = new HashSet<>();
        // normal task first handle current data time
        if (!retry) {
            String current = dataTimeList.remove(dataTimeList.size() - 1);
            dealtDataTime.add(current);
            if (!dealEventMapByDataTime(current, true)) {
                return;
            }
        }
        dataTimeList.forEach(dataTime -> {
            dealtDataTime.add(dataTime);
            if (!dealEventMapByDataTime(dataTime, false)) {
                return;
            }
        });
        for (String dataTime : eventMap.keySet()) {
            if (!dealtDataTime.contains(dataTime)) {
                dealEventMapByDataTime(dataTime, false);
            }
        }
    }

    private boolean dealEventMapByDataTime(String dataTime, boolean isCurrentDataTime) {
        Map<String, InstanceProfile> sameDataTimeEvents = eventMap.get(dataTime);
        if (sameDataTimeEvents == null || sameDataTimeEvents.isEmpty()) {
            return true;
        }
        if (shouldStartNow(dataTime)) {
            Set<InstanceProfile> sortedEvents = new TreeSet<>(Comparator.comparing(InstanceProfile::getInstanceId));
            sortedEvents.addAll(sameDataTimeEvents.values());
            for (InstanceProfile sortEvent : sortedEvents) {
                String fileName = sortEvent.getInstanceId();
                InstanceProfile profile = sameDataTimeEvents.get(fileName);
                if (!isCurrentDataTime && isFull()) {
                    return false;
                }
                if (!instanceQueue.offer(profile)) {
                    return false;
                }
                sameDataTimeEvents.remove(fileName);
            }
        }
        return true;
    }

    /*
     * Calculate whether the event needs to be processed at the current time based on its data time, business cycle, and
     * offset
     */
    private boolean shouldStartNow(String dataTime) {
        String shouldStartTime =
                NewDateUtils.getShouldStartTime(dataTime, taskProfile.getCycleUnit(), timeOffset);
        String currentTime = getCurrentTime();
        return currentTime.compareTo(shouldStartTime) >= 0;
    }

    private void removeTimeoutEvent(Map<String, Map<String, InstanceProfile>> eventMap, boolean isRetry) {
        if (isRetry) {
            return;
        }
        for (Map.Entry<String, Map<String, InstanceProfile>> entry : eventMap.entrySet()) {
            /* If the data time of the event is within 2 days before (after) the current time, it is valid */
            String dataTime = entry.getKey();
            if (!NewDateUtils.isValidCreationTime(dataTime, DAY_TIMEOUT_INTERVAL)) {
                /* Remove it from memory map. */
                eventMap.remove(dataTime);
                LOGGER.warn("remove too old event from event map. dataTime {}", dataTime);
            }
        }
    }

    private String getCurrentTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat(NewDateUtils.DEFAULT_FORMAT);
        TimeZone timeZone = TimeZone.getTimeZone(NewDateUtils.DEFAULT_TIME_ZONE);
        dateFormat.setTimeZone(timeZone);
        return dateFormat.format(new Date(System.currentTimeMillis()));
    }

    private void addToEvenMap(String fileName, String dataTime) {
        if (isInEventMap(fileName, dataTime)) {
            LOGGER.info("addToEvenMap isInEventMap returns true skip taskId {} dataTime {} fileName {}",
                    taskProfile.getTaskId(), dataTime, fileName);
            return;
        }
        ObjectMetadata meta = cosClient.getObjectMetadata(bucketName, fileName);
        Long fileUpdateTime = meta.getLastModified().getTime();
        if (!shouldAddAgain(fileName, fileUpdateTime)) {
            LOGGER.info("addToEvenMap shouldAddAgain returns false skip taskId {} dataTime {} fileName {}",
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
        InstanceProfile instanceProfile = taskProfile.createInstanceProfile(DEFAULT_COS_INSTANCE,
                fileName, cycleUnit, dataTime, fileUpdateTime);
        sameDataTimeEvents.put(fileName, instanceProfile);
        LOGGER.info("add to eventMap taskId {} dataTime {} fileName {}", taskProfile.getTaskId(), dataTime, fileName);
    }
}
