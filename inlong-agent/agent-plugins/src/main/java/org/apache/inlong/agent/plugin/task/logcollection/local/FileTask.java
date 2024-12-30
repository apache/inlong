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

package org.apache.inlong.agent.plugin.task.logcollection.local;

import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.CycleUnitType;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.plugin.task.logcollection.LogAbstractTask;
import org.apache.inlong.agent.plugin.task.logcollection.local.FileScanner.BasicFileInfo;
import org.apache.inlong.agent.plugin.utils.regex.DateUtils;
import org.apache.inlong.agent.plugin.utils.regex.PathDateExpression;
import org.apache.inlong.agent.plugin.utils.regex.PatternUtil;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DateTransUtils;
import org.apache.inlong.agent.utils.file.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Watch directory, if new valid files are created, create instance correspondingly.
 */
public class FileTask extends LogAbstractTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileTask.class);
    private final Map<String, WatchEntity> watchers = new ConcurrentHashMap<>();
    private final Set<String> watchFailedDirs = new HashSet<>();
    public static final int CORE_THREAD_MAX_GAP_TIME_MS = 60 * 1000;
    private boolean realTime = false;
    private Set<String> originPatterns;
    private long lastScanTime = 0;
    public final long SCAN_INTERVAL = 1 * 60 * 1000;
    private volatile long coreThreadUpdateTime = 0;

    @Override
    protected int getInstanceLimit() {
        return taskProfile.getInt(TaskConstants.FILE_MAX_NUM);
    }

    @Override
    protected void initTask() {
        super.initTask();
        timeOffset = taskProfile.get(TaskConstants.TASK_FILE_TIME_OFFSET, "");
        retry = taskProfile.isRetry();
        originPatterns = Stream.of(taskProfile.get(TaskConstants.FILE_DIR_FILTER_PATTERNS).split(","))
                .collect(Collectors.toSet());
        if (taskProfile.getCycleUnit().compareToIgnoreCase(CycleUnitType.REAL_TIME) == 0) {
            realTime = true;
        }
        if (retry) {
            initRetryTask(taskProfile);
        } else {
            watchInit();
        }
    }

    private boolean initRetryTask(TaskProfile profile) {
        String dataTimeFrom = profile.get(TaskConstants.FILE_TASK_TIME_FROM, "");
        String dataTimeTo = profile.get(TaskConstants.FILE_TASK_TIME_TO, "");
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
        if (!profile.hasKey(TaskConstants.FILE_TASK_CYCLE_UNIT)) {
            LOGGER.error("task profile needs file cycle unit");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_CYCLE_UNIT)) {
            LOGGER.error("task profile needs cycle unit");
            return false;
        }
        if (profile.get(TaskConstants.TASK_CYCLE_UNIT)
                .compareTo(profile.get(TaskConstants.FILE_TASK_CYCLE_UNIT)) != 0) {
            LOGGER.error("task profile cycle unit must be consistent");
            return false;
        }
        if (!profile.hasKey(TaskConstants.TASK_TIME_ZONE)) {
            LOGGER.error("task profile needs time zone");
            return false;
        }
        boolean ret = profile.hasKey(TaskConstants.FILE_DIR_FILTER_PATTERNS)
                && profile.hasKey(TaskConstants.FILE_MAX_NUM);
        if (!ret) {
            LOGGER.error("task profile needs file keys");
            return false;
        }
        if (profile.getCycleUnit().compareToIgnoreCase(CycleUnitType.REAL_TIME) != 0 && !profile.hasKey(
                TaskConstants.TASK_FILE_TIME_OFFSET)) {
            LOGGER.error("task profile needs time offset");
            return false;
        }
        if (profile.isRetry()) {
            if (!initRetryTask(profile)) {
                return false;
            }
        }
        return true;
    }

    private void watchInit() {
        originPatterns.forEach((pathPattern) -> {
            addPathPattern(pathPattern);
        });
    }

    public void addPathPattern(String originPattern) {
        ArrayList<String> directories = PatternUtil.cutDirectoryByWildcardAndDateExpression(originPattern);
        String basicStaticPath = directories.get(0);
        LOGGER.info("dataName {} watchPath {}", new Object[]{originPattern, basicStaticPath});
        /* Remember the failed watcher creations. */
        if (!new File(basicStaticPath).exists()) {
            LOGGER.warn("DIRECTORY_NOT_FOUND_ERROR" + basicStaticPath);
            watchFailedDirs.add(originPattern);
            return;
        }
        try {
            /*
             * When we construct the watch object, we should do some work with the data name, replace yyyy to 4 digits
             * regression, mm to 2 digits regression, also because of difference between java regular expression and
             * linux regular expression, we have to replace * to ., and replace . with \\. .
             */
            WatchService watchService = FileSystems.getDefault().newWatchService();
            WatchEntity entity = new WatchEntity(watchService, originPattern, taskProfile.getCycleUnit());
            entity.registerRecursively();
            watchers.put(originPattern, entity);
            watchFailedDirs.remove(originPattern);
        } catch (IOException e) {
            if (e.toString().contains("Too many open files") || e.toString().contains("打开的文件过多")) {
                LOGGER.error("WATCH_DIR_ERROR", e);
            } else {
                LOGGER.error("WATCH_DIR_ERROR", e);
            }
        } catch (Exception e) {
            LOGGER.error("addPathPattern:", e);
        }
    }

    @Override
    protected void releaseTask() {
        releaseWatchers(watchers);
    }

    private void releaseWatchers(Map<String, WatchEntity> watchers) {
        while (running) {
            if (AgentUtils.getCurrentTime() - coreThreadUpdateTime > CORE_THREAD_MAX_GAP_TIME_MS) {
                LOGGER.error("core thread not update, maybe it has broken");
                break;
            }
            AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME);
        }
        watchers.forEach((taskId, watcher) -> {
            try {
                watcher.getWatchService().close();
            } catch (IOException e) {
                LOGGER.error("close watch service failed taskId {}", e, taskId);
            }
        });
    }

    @Override
    protected void runForNormal() {
        if (AgentUtils.getCurrentTime() - lastScanTime > SCAN_INTERVAL) {
            scanExistingFile();
            lastScanTime = AgentUtils.getCurrentTime();
        }
        runForWatching();
        dealWithEventMap();
    }

    @Override
    protected void scanExistingFile() {
        originPatterns.forEach((originPattern) -> {
            List<BasicFileInfo> fileInfos = scanExistingFileByPattern(originPattern);
            LOGGER.info("taskId {} scan {} get file count {}", getTaskId(), originPattern, fileInfos.size());
            fileInfos.forEach((fileInfo) -> {
                String fileName = fileInfo.fileName;
                Long fileUpdateTime = FileUtils.getFileLastModifyTime(fileName);
                addToEvenMap(fileName, fileInfo.dataTime, fileUpdateTime, taskProfile.getCycleUnit());
                if (retry) {
                    instanceCount++;
                }
            });
        });
    }

    private List<BasicFileInfo> scanExistingFileByPattern(String originPattern) {
        if (realTime) {
            return FileScanner.scanTaskBetweenTimes(originPattern, CycleUnitType.HOUR, timeOffset,
                    startTime, endTime, retry);
        } else {
            return FileScanner.scanTaskBetweenTimes(originPattern, taskProfile.getCycleUnit(),
                    timeOffset, startTime, endTime, retry);
        }
    }

    private void runForWatching() {
        /* Deal with those failed watcher creation tasks. */
        Set<String> tmpWatchFailedDirs = new HashSet<>(watchFailedDirs);
        for (String originPattern : tmpWatchFailedDirs) {
            addPathPattern(originPattern);
        }
        /*
         * Visit the watchers to see if it gets any new file creation, if it exists and fits the file name pattern, add
         * it to the task list.
         */
        for (Map.Entry<String, WatchEntity> entry : watchers.entrySet()) {
            dealWithWatchEntity(entry.getKey());
        }
    }

    @Override
    protected void dealWithEventMap() {
        removeTimeoutEvent(eventMap, retry);
        if (realTime) {
            dealWithEventMapRealTime();
        } else {
            dealWithEventMapWithCycle();
        }
    }

    private void dealWithEventMapRealTime() {
        for (String dataTime : eventMap.keySet()) {
            dealEventMapByDataTime(dataTime, true);
        }
    }

    public synchronized void dealWithWatchEntity(String originPattern) {
        WatchEntity entity = watchers.get(originPattern);
        if (entity == null) {
            LOGGER.error("Can't find the watch entity for originPattern: " + originPattern);
            return;
        }
        try {
            entity.removeDeletedWatchDir();
            /* Get all creation events until all events are consumed. */
            for (int i = 0; i < entity.getTotalPathSize(); i++) {
                // maybe the watchService is closed ,but we catch this exception!
                final WatchKey key = entity.getWatchService().poll();
                if (key == null) {
                    return;
                }
                dealWithWatchKey(entity, key);
            }
        } catch (Exception e) {
            LOGGER.error("deal with creation event error: ", e);
        }
    }

    private void dealWithWatchKey(WatchEntity entity, WatchKey key) throws IOException {
        Path contextPath = entity.getPath(key);
        LOGGER.info("Find creation events in path: {}", contextPath.toAbsolutePath());
        for (WatchEvent<?> watchEvent : key.pollEvents()) {
            Path child = resolvePathFromEvent(watchEvent, contextPath);
            if (child == null) {
                continue;
            }
            if (Files.isDirectory(child)) {
                LOGGER.info("The find creation event is triggered by a directory: {}", child.getFileName());
                entity.registerRecursively(child);
                continue;
            }
            handleFilePath(child, entity);
        }
        resetWatchKey(entity, key, contextPath);
    }

    private Path resolvePathFromEvent(WatchEvent<?> watchEvent, Path contextPath) {
        final Kind<?> kind = watchEvent.kind();
        /*
         * Can't simply continue when it detects that an event maybe ignored.
         */
        if (kind == StandardWatchEventKinds.OVERFLOW) {
            LOGGER.error("An event is unclear and lost");
            /*
             * TODO: should we do a full scan to avoid lost events?
             */
            return null;
        }
        final WatchEvent<Path> watchEventPath = (WatchEvent<Path>) watchEvent;
        final Path eventPath = watchEventPath.context();
        /*
         * Must resolve, otherwise we can't get the file attributes.
         */
        return contextPath.resolve(eventPath);
    }

    private void handleFilePath(Path filePath, WatchEntity entity) {
        String newFileName = filePath.toFile().getAbsolutePath();
        LOGGER.info("new file {} {}", newFileName, entity.getPattern());
        Matcher matcher = entity.getPattern().matcher(newFileName);
        if (matcher.matches() || matcher.lookingAt()) {
            LOGGER.info("matched file {} {}", newFileName, entity.getPattern());
            String dataTime = getDataTimeFromFileName(newFileName, entity.getDateExpression());
            if (!checkFileNameForTime(newFileName, entity)) {
                LOGGER.error("File Timeout {} {}", newFileName, dataTime);
                return;
            }
            Long fileUpdateTime = FileUtils.getFileLastModifyTime(newFileName);
            addToEvenMap(newFileName, dataTime, fileUpdateTime, taskProfile.getCycleUnit());
        }
    }

    private boolean checkFileNameForTime(String newFileName, WatchEntity entity) {
        /* Get the data time for this file. */
        PathDateExpression dateExpression = entity.getDateExpression();
        if (dateExpression.getLongestDatePattern().length() != 0) {
            String dataTime = getDataTimeFromFileName(newFileName, dateExpression);
            LOGGER.info("file {}, fileTime {}", newFileName, dataTime);
            if (!DateUtils.isValidCreationTime(dataTime, entity.getCycleUnit(), timeOffset)) {
                return false;
            }
        }
        return true;
    }

    private String getDataTimeFromFileName(String fileName, PathDateExpression dateExpression) {
        /*
         * TODO: what if this file doesn't have any date pattern regex.
         *
         * For this case, we can simple think that the next file creation means the last task of this conf should finish
         * reading and start reading this new file.
         */
        // Extract data time from file name
        String fileTime = DateUtils.getDateTime(fileName, dateExpression);

        /**
         * Replace any non-numeric characters in the file time
         * such as 2015-09-16_00 replace with 2015091600
         */
        return fileTime.replaceAll("\\D", "");
    }

    private void resetWatchKey(WatchEntity entity, WatchKey key, Path contextPath) {
        key.reset();
        /*
         * Register a new watch service on the path if the old watcher is invalid.
         */
        if (!key.isValid()) {
            LOGGER.warn("Invalid Watcher {}", contextPath.getFileName());
            try {
                WatchService oldService = entity.getWatchService();
                oldService.close();
                WatchService watchService = FileSystems.getDefault().newWatchService();
                entity.clearKeys();
                entity.clearPathToKeys();
                entity.setWatchService(watchService);
                entity.registerRecursively();
            } catch (IOException e) {
                LOGGER.error("Restart a new watcher runs into error: ", e);
            }
        }
    }
}
