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

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.core.task.TaskAction;
import org.apache.inlong.agent.plugin.task.AbstractTask;
import org.apache.inlong.agent.plugin.utils.regex.NewDateUtils;
import org.apache.inlong.agent.plugin.utils.regex.Scanner;
import org.apache.inlong.agent.state.State;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public abstract class LogAbstractTask extends AbstractTask {

    private static final int INSTANCE_QUEUE_CAPACITY = 10;
    public static final long DAY_TIMEOUT_INTERVAL = 2 * 24 * 3600 * 1000;
    private static final Logger LOGGER = LoggerFactory.getLogger(LogAbstractTask.class);
    protected boolean retry;
    protected BlockingQueue<InstanceProfile> instanceQueue;
    private volatile boolean runAtLeastOneTime = false;
    protected long startTime;
    protected long endTime;
    protected String timeOffset = "";
    protected final Map<String/* dataTime */, Map<String/* fileName */, InstanceProfile>> eventMap =
            new ConcurrentHashMap<>();

    @Override
    protected void initTask() {
        instanceQueue = new LinkedBlockingQueue<>(INSTANCE_QUEUE_CAPACITY);
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

    abstract protected void runForNormal();

    abstract protected void dealWithEventMap();

    abstract protected void scanExistingFile();

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

    protected void dealWithEventMapWithCycle() {
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

    protected boolean dealEventMapByDataTime(String dataTime, boolean isCurrentDataTime) {
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
        String shouldStartTime = NewDateUtils.getShouldStartTime(dataTime, taskProfile.getCycleUnit(), timeOffset);
        String currentTime = getCurrentTime();
        return currentTime.compareTo(shouldStartTime) >= 0;
    }

    private String getCurrentTime() {
        SimpleDateFormat dateFormat = new SimpleDateFormat(NewDateUtils.DEFAULT_FORMAT);
        TimeZone timeZone = TimeZone.getTimeZone(NewDateUtils.DEFAULT_TIME_ZONE);
        dateFormat.setTimeZone(timeZone);
        return dateFormat.format(new Date(System.currentTimeMillis()));
    }

    protected void removeTimeoutEvent(Map<String, Map<String, InstanceProfile>> eventMap, boolean isRetry) {
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
}
