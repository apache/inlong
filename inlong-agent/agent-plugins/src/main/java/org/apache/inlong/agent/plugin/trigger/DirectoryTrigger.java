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

package org.apache.inlong.agent.plugin.trigger;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.conf.TriggerProfile;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.constant.FileTriggerType;
import org.apache.inlong.agent.constant.JobConstants;
import org.apache.inlong.agent.plugin.Trigger;
import org.apache.inlong.agent.plugin.utils.PluginUtils;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.inlong.agent.constant.JobConstants.JOB_DIR_FILTER_BLACKLIST;
import static org.apache.inlong.agent.constant.JobConstants.JOB_DIR_FILTER_PATTERNS;

/**
 * Watch directory, if new valid files are created, create jobs correspondingly.
 */
public class DirectoryTrigger extends AbstractDaemon implements Trigger {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirectoryTrigger.class);
    private static volatile WatchService watchService;
    private final ConcurrentHashMap<PathPattern, List<WatchKey>> allWatchers =
            new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<JobProfile> queue = new LinkedBlockingQueue<>();
    private TriggerProfile profile;
    private int interval;

    private static void initWatchService() {
        try {
            if (watchService == null) {
                synchronized (DirectoryTrigger.class) {
                    if (watchService == null) {
                        watchService = FileSystems.getDefault().newWatchService();
                        LOGGER.info("init watch service {}", watchService);
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.warn("error while init watch service", ex);
        }
    }

    public TriggerProfile getProfile() {
        return profile;
    }

    @Override
    public void destroy() {
        try {
            stop();
        } catch (Exception ex) {
            LOGGER.error("exception while stopping threads", ex);
        }
    }

    @Override
    public JobProfile fetchJobProfile() {
        return queue.poll();
    }

    @Override
    public TriggerProfile getTriggerProfile() {
        return profile;
    }

    @Override
    public void stop() {
        waitForTerminate();
        releaseResource();
    }

    /**
     * register all sub-directory
     *
     * @param entity entity
     * @param path path
     * @param tmpWatchers watchers
     */
    private void registerAllSubDir(PathPattern entity,
            Path path,
            List<WatchKey> tmpWatchers) throws IOException {
        // check regex
        LOGGER.info("check whether path {} is suitable", path);
        if (entity.suitable(path.toString())) {
            LOGGER.info("path {} is suitable.", path);
            if (path.toFile().isDirectory()) {
                WatchKey watchKey = path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
                tmpWatchers.add(watchKey);
                try (Stream<Path> stream = Files.list(path)) {
                    Iterator<Path> iterator = stream.iterator();
                    while (iterator.hasNext()) {
                        registerAllSubDir(entity, iterator.next().toAbsolutePath(), tmpWatchers);
                    }
                }
            } else {
                JobProfile copiedJobProfile = PluginUtils.copyJobProfile(profile,
                        entity.getSuitTime(), path.toFile());
                LOGGER.info("trigger {} generate job profile to read file {}",
                        getTriggerProfile().getTriggerId(), path);
                queue.offer(copiedJobProfile);
            }
        }
    }

    /**
     * if directory has created, then check whether directory is valid
     *
     * @param entity entity
     * @param watchKey watch key
     * @param tmpWatchers watchers
     */
    private void registerNewDir(PathPattern entity,
            WatchKey watchKey,
            List<WatchKey> tmpWatchers,
            List<WatchKey> tmpDeletedWatchers) throws Exception {
        Path parentPath = (Path) watchKey.watchable();
        for (WatchEvent<?> event : watchKey.pollEvents()) {
            // if watch event is too much, then event would be overflow.
            if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
                // only watch create event, so else is create-event.
                entity.updateDateFormatRegex();
                Path createdPath = (Path) event.context();
                if (createdPath != null) {
                    registerAllSubDir(entity, parentPath.resolve(createdPath), tmpWatchers);
                }
            } else if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
                LOGGER.info("overflow got {}", parentPath);
                // check whether parent path is valid.
                if (Files.isDirectory(parentPath)) {
                    try (final Stream<Path> pathStream = Files.list(parentPath)) {
                        for (Iterator<Path> it = pathStream.iterator(); it.hasNext();) {
                            Path childPath = it.next();
                            registerAllSubDir(entity, parentPath.resolve(childPath), tmpWatchers);
                        }
                    } catch (Exception e) {
                        LOGGER.error("error caught", e);
                    }
                }
            }
        }
        if (!Files.exists(parentPath)) {
            LOGGER.warn("{} not exist, add watcher to pending delete list", parentPath);
            tmpDeletedWatchers.add(watchKey);
        }
    }

    /**
     * handler watchers
     *
     * @return runnable
     */
    private Runnable watchEventHandler() {
        return () -> {
            while (isRunnable()) {
                try {
                    TimeUnit.SECONDS.sleep(interval);
                    allWatchers.forEach((pathPattern, watchKeys) -> {
                        List<WatchKey> tmpWatchers = new ArrayList<>();
                        List<WatchKey> tmpDeletedWatchers = new ArrayList<>();
                        pathPattern.cleanup();
                        try {
                            for (WatchKey watchKey : watchKeys) {
                                registerNewDir(pathPattern, watchKey, tmpWatchers, tmpDeletedWatchers);
                            }
                        } catch (Exception ex) {
                            LOGGER.error("error caught", ex);
                        }
                        watchKeys.addAll(tmpWatchers);
                        watchKeys.removeAll(tmpDeletedWatchers);
                    });
                } catch (Throwable ex) {
                    LOGGER.error("error caught", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                }
            }
        };
    }

    private void releaseResource() {
        allWatchers.forEach((absoluteFilePath, watchKeys) -> {
            watchKeys.forEach(WatchKey::cancel);
        });
        allWatchers.clear();
    }

    @Override
    public void start() throws Exception {
        submitWorker(watchEventHandler());
    }

    /**
     * register pathPattern into watchers, with offset
     */
    public Set<String> register(Set<String> whiteList, String offset, Set<String> blackList) throws IOException {
        Set<PathPattern> pathPatterns = PathPattern.buildPathPattern(whiteList, offset, blackList);
        for (PathPattern pathPattern : pathPatterns) {
            innerRegister(pathPattern.getRootDir(), pathPattern);
        }
        return pathPatterns.stream().map(PathPattern::getRootDir).collect(Collectors.toSet());
    }

    private void innerRegister(String watchDir, PathPattern entity) throws IOException {
        List<WatchKey> tmpKeyList = new ArrayList<>();
        List<WatchKey> keyList = allWatchers.putIfAbsent(entity, tmpKeyList);
        if (keyList != null) {
            LOGGER.error("{} exists in watcher list, please check it", watchDir);
            return;
        }

        Path rootPath = Paths.get(entity.getRootDir());
        LOGGER.info("watch root path is {}", rootPath);
        WatchKey key = rootPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
        if (FileTriggerType.FULL.equals(
                profile.get(JobConstants.JOB_FILE_TRIGGER_TYPE, FileTriggerType.FULL))) {
            registerAllSubDir(entity, Paths.get(entity.getRootDir()), tmpKeyList);
        }
        tmpKeyList.add(key);
    }

    public void unregister(String watchDir) {
        Collection<WatchKey> allKeys = allWatchers.remove(
                new PathPattern(watchDir, Collections.emptySet(), Collections.emptySet()));
        if (allKeys != null) {
            LOGGER.info("unregister pattern {}, total size of path {}", watchDir, allKeys.size());
            for (WatchKey key : allKeys) {
                key.cancel();
            }
        }
    }

    ConcurrentHashMap<PathPattern, List<WatchKey>> getAllWatchers() {
        return allWatchers;
    }

    @Override
    public void init(TriggerProfile profile) throws IOException {
        initWatchService();
        interval = profile.getInt(
                AgentConstants.TRIGGER_CHECK_INTERVAL, AgentConstants.DEFAULT_TRIGGER_CHECK_INTERVAL);
        this.profile = profile;
        if (this.profile.hasKey(JOB_DIR_FILTER_PATTERNS)) {
            Set<String> pathPatterns = Stream.of(
                    this.profile.get(JOB_DIR_FILTER_PATTERNS).split(",")).collect(Collectors.toSet());
            Set<String> blackList = Stream.of(
                    this.profile.get(JOB_DIR_FILTER_BLACKLIST, "").split(","))
                    .filter(black -> !StringUtils.isBlank(black))
                    .collect(Collectors.toSet());
            String timeOffset = this.profile.get(JobConstants.JOB_FILE_TIME_OFFSET, "");
            register(pathPatterns, timeOffset, blackList);
        }
    }

    @Override
    public void run() {
        try {
            start();
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    @VisibleForTesting
    public Collection<JobProfile> getFetchedJob() {
        return queue;
    }
}
