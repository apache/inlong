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

package org.apache.inlong.tubemq.server.broker.offset.offsetfile;

import org.apache.inlong.tubemq.corebase.daemon.AbstractDaemonService;
import org.apache.inlong.tubemq.corebase.rv.RetValue;
import org.apache.inlong.tubemq.corebase.utils.ConcurrentHashSet;
import org.apache.inlong.tubemq.corebase.utils.ServiceStatusHolder;
import org.apache.inlong.tubemq.server.broker.offset.OffsetHistoryInfo;
import org.apache.inlong.tubemq.server.broker.offset.OffsetStorage;
import org.apache.inlong.tubemq.server.broker.offset.OffsetStorageInfo;
import org.apache.inlong.tubemq.server.broker.stats.BrokerSrvStatsHolder;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class FileOffsetStorage extends AbstractDaemonService implements OffsetStorage {

    private static final Logger logger = LoggerFactory.getLogger(FileOffsetStorage.class);
    private static final Gson GSON = new Gson();
    private static final String offsetSubDir = "offsetDir";
    private static final String OFFSET_FILENAME = "offsets";
    private static final String OFFSET_FILENAME_SUFFIX_FORMAL = ".meta";
    private static final String OFFSET_FILENAME_SUFFIX_TMP = ".tmp";
    private static final String OFFSET_FILENAME_SUFFIX_MID = ".mid";
    private static final String OFFSET_FILENAME_SUFFIX_OLD = ".old";
    private final int brokerId;
    private final String offsetsDirBase;
    private final String offsetsFileBase;
    private final long syncDurWarnMs;
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final AtomicBoolean isUpdated = new AtomicBoolean(false);
    private GroupOffsetStgInfo groupOffsetInfo;
    private final ConcurrentHashMap<String, ConcurrentHashSet<String>> groupTopicsInfo = new ConcurrentHashMap<>();

    public FileOffsetStorage(int brokerId, String offsetFilePath, long syncIntMs, long syncDurWarnMs) {
        super("Offset-File", syncIntMs);
        this.brokerId = brokerId;
        this.syncDurWarnMs = syncDurWarnMs;
        this.offsetsDirBase = offsetFilePath + File.separator + offsetSubDir;
        this.offsetsFileBase = this.offsetsDirBase + File.separator + OFFSET_FILENAME;
    }

    @Override
    public void start() {
        if (!this.isStarted.compareAndSet(false, true)) {
            return;
        }
        logger.info("[File offsets] initial File offsets starting...");
        super.start();
        if (!initialFileInfo()) {
            logger.error("[File offsets] initial File offsets failure!");
            System.exit(2);
            return;
        }
        this.isStarted.set(true);
        logger.info("[File offsets] initial File offsets started!");
    }

    @Override
    protected void loopProcess(StringBuilder strBuff) {
        if (!this.isStarted.get()) {
            return;
        }
        if (!this.isUpdated.compareAndSet(true, false)) {
            return;
        }
        long curStartTime = System.currentTimeMillis();
        storeOffsetStgInfoToFile(this.groupOffsetInfo, this.offsetsFileBase);
        long wastMs = System.currentTimeMillis() - curStartTime;
        if (wastMs > syncDurWarnMs) {
            logger.warn("[File offsets] sync offsets to file over warn value, wast={}ms, warnMs={}",
                    wastMs, syncDurWarnMs);
        }
    }

    @Override
    public void close() {
        if (!this.isStarted.compareAndSet(true, false)) {
            return;
        }
        super.stop();
        logger.info("[File offsets] begin sync content to file, begin");
        long curStartTime = System.currentTimeMillis();
        storeOffsetStgInfoToFile(this.groupOffsetInfo, this.offsetsFileBase);
        long wastMs = System.currentTimeMillis() - curStartTime;
        if (wastMs > syncDurWarnMs) {
            logger.warn("[File offsets] close and sync offsets to file, wast={}ms, warnMs={}",
                    wastMs, syncDurWarnMs);
        }
    }

    @Override
    public ConcurrentHashMap<String, OffsetStorageInfo> loadGroupStgInfo(String group) {
        ConcurrentHashMap<String, OffsetStorageInfo> result = new ConcurrentHashMap<>();
        if (!this.isStarted.get()) {
            return result;
        }
        Map<String, PartStgInfo> partStgInfos = this.groupOffsetInfo.getOffsetStgInfos(group);
        if (partStgInfos == null) {
            return result;
        }
        for (Map.Entry<String, PartStgInfo> entry : partStgInfos.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            PartStgInfo partStgInfo = entry.getValue();
            result.put(entry.getKey(), new OffsetStorageInfo(partStgInfo.getTopic(),
                    brokerId, partStgInfo.getPartId(), partStgInfo.getLstRstTerm(),
                    partStgInfo.getLstOffset(), partStgInfo.getMsgId(), false,
                    partStgInfo.getLstUpdTime()));
        }
        return result;
    }

    @Override
    public OffsetStorageInfo loadOffset(String group, String topic, int partitionId) {
        if (!this.isStarted.get()) {
            return null;
        }
        PartStgInfo partStgInfo =
                this.groupOffsetInfo.getOffsetStgInfo(group, topic, partitionId);
        if (partStgInfo == null) {
            return null;
        }
        return new OffsetStorageInfo(topic, brokerId, partitionId,
                partStgInfo.getLstRstTerm(), partStgInfo.getLstOffset(),
                partStgInfo.getMsgId(), false, partStgInfo.getLstUpdTime());
    }

    @Override
    public boolean commitOffset(String group, Collection<OffsetStorageInfo> offsetInfoList, boolean isFailRetry) {
        if (offsetInfoList == null || offsetInfoList.isEmpty()) {
            return false;
        }
        if (this.groupOffsetInfo.storeOffsetStgInfo(
                group, offsetInfoList, this.groupTopicsInfo)) {
            isUpdated.set(true);
            return true;
        }
        return false;
    }

    @Override
    public Map<String, Set<String>> queryGroupTopicInfo(Set<String> groups) {
        if (this.groupTopicsInfo.isEmpty()) {
            return Collections.emptyMap();
        }
        Set<String> tmpTopics;
        Map<String, Set<String>> groupTopicMap;
        if (groups == null) {
            groupTopicMap = new HashMap<>();
            for (Map.Entry<String, ConcurrentHashSet<String>> entry : this.groupTopicsInfo.entrySet()) {
                if (entry == null
                        || entry.getKey() == null
                        || entry.getValue() == null
                        || entry.getValue().isEmpty()) {
                    continue;
                }
                tmpTopics = groupTopicMap.computeIfAbsent(entry.getKey(), k -> new HashSet<>());
                tmpTopics.addAll(entry.getValue());
            }
            return groupTopicMap;
        } else {
            if (groups.isEmpty()) {
                return Collections.emptyMap();
            }
            groupTopicMap = new HashMap<>();
            for (Map.Entry<String, ConcurrentHashSet<String>> entry : this.groupTopicsInfo.entrySet()) {
                if (entry == null
                        || entry.getKey() == null
                        || entry.getValue() == null
                        || entry.getValue().isEmpty()) {
                    continue;
                }
                if (groups.contains(entry.getKey())) {
                    tmpTopics = groupTopicMap.computeIfAbsent(entry.getKey(), k -> new HashSet<>());
                    tmpTopics.addAll(entry.getValue());
                }
            }
            return groupTopicMap;
        }
    }

    @Override
    public Map<Integer, Long> queryGroupOffsetInfo(String group, String topic, Set<Integer> partitionIds) {
        return this.groupOffsetInfo.queryGroupOffsetInfo(group, topic, partitionIds);
    }

    @Override
    public void deleteGroupOffsetInfo(Map<String, Map<String, Set<Integer>>> groupTopicPartMap) {
        if (groupTopicPartMap == null || groupTopicPartMap.isEmpty()) {
            return;
        }
        boolean isUpdated = false;
        ConcurrentHashSet<String> curTopics;
        Set<String> rmvTopics = new HashSet<>();
        for (Map.Entry<String, Map<String, Set<Integer>>> entry : groupTopicPartMap.entrySet()) {
            if (entry == null
                    || entry.getKey() == null
                    || entry.getValue() == null
                    || entry.getValue().isEmpty()) {
                continue;
            }
            if (this.groupOffsetInfo.rmvGroupOffsetInfo(entry.getKey(), entry.getValue())) {
                isUpdated = true;
            }
            // clean cache
            curTopics = groupTopicsInfo.get(entry.getKey());
            if (curTopics == null) {
                continue;
            }
            for (String topic : curTopics) {
                if (topic == null) {
                    continue;
                }
                if (this.groupOffsetInfo.includedTopic(entry.getKey(), topic)) {
                    rmvTopics.add(topic);
                }
            }
            if (!rmvTopics.isEmpty()) {
                for (String topic : rmvTopics) {
                    curTopics.remove(topic);
                }
                if (curTopics.isEmpty()) {
                    groupTopicsInfo.remove(entry.getKey());
                }
            }
        }
        if (isUpdated) {
            this.isUpdated.set(true);
        }
    }

    @Override
    public Set<String> cleanExpiredGroupInfo(long checkTime, long expiredDurMs) {
        Set<String> rmvGroups =
                this.groupOffsetInfo.rmvExpiredGroupOffsetInfo(checkTime, expiredDurMs);
        if (!rmvGroups.isEmpty()) {
            for (String rmvGroup : rmvGroups) {
                this.groupTopicsInfo.remove(rmvGroup);
            }
            this.isUpdated.set(true);
        }
        return rmvGroups;
    }

    @Override
    public Set<String> cleanRmvTopicInfo(Set<String> rmvTopics) {
        Set<String> groups = new HashSet<>();
        for (String topic : rmvTopics) {
            if (topic == null) {
                continue;
            }
            for (Map.Entry<String, ConcurrentHashSet<String>> entry : this.groupTopicsInfo.entrySet()) {
                if (entry == null
                        || entry.getKey() == null
                        || entry.getValue() == null
                        || entry.getValue().isEmpty()) {
                    continue;
                }
                if (entry.getValue().contains(topic)) {
                    groups.add(entry.getKey());
                }
            }
        }
        if (groups.isEmpty()) {
            return groups;
        }
        ConcurrentHashSet<String> curTopics;
        for (String group : groups) {
            this.groupOffsetInfo.rmvGroupOffsetInfo(group, rmvTopics);
            curTopics = groupTopicsInfo.get(group);
            if (curTopics == null) {
                continue;
            }
            curTopics.addAll(rmvTopics);
            if (curTopics.isEmpty()) {
                groupTopicsInfo.remove(group);
            }
        }
        this.isUpdated.set(true);
        return groups;
    }

    public String getOffsetsFileBase() {
        return offsetsFileBase;
    }

    public Map<String, OffsetHistoryInfo> getOffsetHistoryInfo(Set<String> groups) {
        if (groups == null || groups.isEmpty()) {
            return Collections.emptyMap();
        }
        OffsetStgInfo curStgInfo;
        OffsetHistoryInfo tmpHisInfo;
        ConcurrentHashMap<String, PartStgInfo> curPartStgInfoMap;
        Map<String, OffsetHistoryInfo> result = new HashMap<>();
        ConcurrentHashMap<String, OffsetStgInfo> offsetStgInfoMap =
                this.groupOffsetInfo.getGroupOffsetStgInfo();
        for (String group : groups) {
            curStgInfo = offsetStgInfoMap.get(group);
            if (curStgInfo == null || curStgInfo.getPartOffsetInfo().isEmpty()) {
                continue;
            }
            tmpHisInfo = new OffsetHistoryInfo(brokerId, group);
            curPartStgInfoMap = curStgInfo.getPartOffsetInfo();
            for (PartStgInfo partStgInfo : curPartStgInfoMap.values()) {
                if (partStgInfo == null) {
                    continue;
                }
                tmpHisInfo.addCsmOffsets(partStgInfo.getTopic(),
                        partStgInfo.getPartId(), partStgInfo.getLstOffset(), 0L);
            }
            result.put(group, tmpHisInfo);
        }
        return result;
    }

    public boolean isFistUseFileStg() {
        boolean isFistUseFileStg = false;
        File baseDir = new File(this.offsetsDirBase);
        if (!baseDir.exists()) {
            if (!baseDir.mkdirs()) {
                logger.error("[File offsets] could not make File offset directory {}",
                        baseDir.getAbsolutePath());
                System.exit(2);
            }
            isFistUseFileStg = true;
        }
        if (!baseDir.isDirectory() || !baseDir.canRead()) {
            logger.error("[File offsets] File offset path {} is not a readable directory",
                    baseDir.getAbsolutePath());
            System.exit(2);
        }
        final File[] listFiles = baseDir.listFiles();
        if (listFiles != null) {
            isFistUseFileStg = true;
            for (File file : listFiles) {
                if (file.isFile()) {
                    if (file.getName().endsWith(OFFSET_FILENAME_SUFFIX_MID)
                            || file.getName().endsWith(OFFSET_FILENAME_SUFFIX_FORMAL)
                            || file.getName().endsWith(OFFSET_FILENAME_SUFFIX_OLD)) {
                        isFistUseFileStg = false;
                        break;
                    }
                }
            }
        }
        return isFistUseFileStg;
    }

    private boolean initialFileInfo() {
        if (!checkAndRecoverStgFiles()) {
            return false;
        }
        GroupOffsetStgInfo tmpOffsetInfoMap;
        File dstFile = new File(this.offsetsFileBase + OFFSET_FILENAME_SUFFIX_FORMAL);
        if (dstFile.exists()) {
            String offsetsContent = getConfigFromFile(dstFile);
            if (offsetsContent == null) {
                logger.error("[File offsets] initial load storage file {} failure!",
                        dstFile.getAbsoluteFile());
                return false;
            }
            try {
                tmpOffsetInfoMap = GSON.fromJson(offsetsContent, GroupOffsetStgInfo.class);
            } catch (Throwable ex) {
                logger.error("[File offsets] parse loaded json config failure", ex);
                return false;
            }
            if (tmpOffsetInfoMap == null) {
                logger.error("[File offsets] LOADED configure is null");
                return false;
            }
        } else {
            if (!isFistUseFileStg()) {
                logger.error("[File offsets] storage file {} is required!",
                        dstFile.getAbsoluteFile());
                return false;
            }
            tmpOffsetInfoMap = new GroupOffsetStgInfo(this.brokerId);
            RetValue retValue = storeOffsetStgInfoToFile(tmpOffsetInfoMap, this.offsetsFileBase);
            if (!retValue.isSuccess()) {
                return false;
            }
        }
        this.groupOffsetInfo = tmpOffsetInfoMap;
        Map<String, OffsetStgInfo> offsetStgInfos = tmpOffsetInfoMap.getGroupOffsetStgInfo();
        if (offsetStgInfos == null || offsetStgInfos.isEmpty()) {
            return true;
        }
        ConcurrentHashSet<String> tmpSet;
        ConcurrentHashSet<String> topicSet;
        for (Map.Entry<String, OffsetStgInfo> entry : offsetStgInfos.entrySet()) {
            if (entry == null
                    || entry.getKey() == null
                    || entry.getValue() == null) {
                continue;
            }
            Map<String, PartStgInfo> partStgInfoMap = entry.getValue().getPartOffsetInfo();
            if (partStgInfoMap == null || partStgInfoMap.isEmpty()) {
                continue;
            }
            for (Map.Entry<String, PartStgInfo> partEntry : partStgInfoMap.entrySet()) {
                if (partEntry == null || partEntry.getValue() == null) {
                    continue;
                }
                topicSet = groupTopicsInfo.get(entry.getKey());
                if (topicSet == null) {
                    tmpSet = new ConcurrentHashSet<>();
                    topicSet = groupTopicsInfo.putIfAbsent(entry.getKey(), tmpSet);
                    if (topicSet == null) {
                        topicSet = tmpSet;
                    }
                }
                topicSet.add(partEntry.getValue().getTopic());
            }
        }
        return true;
    }

    private boolean checkAndRecoverStgFiles() {
        String fileContent;
        GroupOffsetStgInfo tmpGroupStgInfo;
        File midFile = new File(this.offsetsFileBase + OFFSET_FILENAME_SUFFIX_MID);
        File dstFile = new File(this.offsetsFileBase + OFFSET_FILENAME_SUFFIX_FORMAL);
        File oldFile = new File(this.offsetsFileBase + OFFSET_FILENAME_SUFFIX_OLD);
        // delete tmp file
        FileUtils.deleteQuietly(new File(this.offsetsFileBase + OFFSET_FILENAME_SUFFIX_TMP));
        try {
            if (midFile.exists()) {
                fileContent = getConfigFromFile(midFile);
                if (fileContent == null) {
                    logger.error("[File offsets] load mid file {} failure!", midFile.getAbsoluteFile());
                    return false;
                }
                tmpGroupStgInfo = getOffsetStgInfoFromConfig(fileContent);
                if (tmpGroupStgInfo == null) {
                    logger.error("[File offsets] parse loaded mid file {} failure!", midFile.getAbsoluteFile());
                    return false;
                }
                tmpGroupStgInfo.clear();
                if (dstFile.exists()) {
                    if (!dstFile.delete()) {
                        logger.error("[File offsets] delete residual file1 {} failed!", dstFile.getAbsoluteFile());
                        return false;
                    }
                }
                if (oldFile.exists()) {
                    if (!oldFile.delete()) {
                        logger.error("[File offsets] delete residual file2 {} failed!", oldFile.getAbsoluteFile());
                        return false;
                    }
                }
                if (!storeConfigToFile(fileContent, oldFile)) {
                    logger.error("[File offsets] backup content to file {} failed!", oldFile.getAbsoluteFile());
                    return false;
                }
                if (!oldFile.exists()) {
                    logger.error("[File offsets] backup file {} not found!", oldFile.getAbsoluteFile());
                    return false;
                }
                FileUtils.moveFile(midFile, dstFile);
                if (!dstFile.exists()) {
                    logger.error("[File offsets] moved formal file {} not found!", dstFile.getAbsoluteFile());
                    return false;
                }
                if (!oldFile.delete()) {
                    logger.error("[File offsets] delete backup file {} failure!", oldFile.getAbsoluteFile());
                    return false;
                }
                return true;
            }
            if (dstFile.exists()) {
                fileContent = getConfigFromFile(dstFile);
                if (fileContent == null) {
                    logger.error("[File offsets] load formal file {} failure!", dstFile.getAbsoluteFile());
                    return false;
                }
                tmpGroupStgInfo = getOffsetStgInfoFromConfig(fileContent);
                if (tmpGroupStgInfo == null) {
                    logger.error("[File offsets] parse loaded formal file {} failure!", dstFile.getAbsoluteFile());
                    return false;
                }
                if (oldFile.exists()) {
                    if (!oldFile.delete()) {
                        logger.error("[File offsets] delete residual file {} failed!", oldFile.getAbsoluteFile());
                        return false;
                    }
                }
                return true;
            }
            if (oldFile.exists()) {
                fileContent = getConfigFromFile(oldFile);
                if (fileContent == null) {
                    logger.error("[File offsets] load backup file {} failure!",
                            oldFile.getAbsoluteFile());
                    return false;
                }
                tmpGroupStgInfo = getOffsetStgInfoFromConfig(fileContent);
                if (tmpGroupStgInfo == null) {
                    logger.error("[File offsets] parse loaded backup content {} failure!",
                            oldFile.getAbsoluteFile());
                    return false;
                }
                tmpGroupStgInfo.clear();
                if (!storeConfigToFile(fileContent, dstFile)) {
                    logger.error("[File offsets] recover formal file {} failed!",
                            dstFile.getAbsoluteFile());
                    return false;
                }
                if (!dstFile.exists()) {
                    logger.error("[File offsets] recovered formal file {} not found!",
                            dstFile.getAbsoluteFile());
                    return false;
                }
                fileContent = getConfigFromFile(dstFile);
                if (fileContent == null) {
                    logger.error("[File offsets] load recovered file {} failure!",
                            dstFile.getAbsoluteFile());
                    return false;
                }
                tmpGroupStgInfo = getOffsetStgInfoFromConfig(fileContent);
                if (tmpGroupStgInfo == null) {
                    logger.error("[File offsets] parse recovered file content {} failure!",
                            dstFile.getAbsoluteFile());
                    return false;
                }
                tmpGroupStgInfo.clear();
                if (!oldFile.delete()) {
                    logger.error("[File offsets] delete backup file {} failed!",
                            oldFile.getAbsoluteFile());
                    return false;
                }
            }
            return true;
        } catch (Throwable ex) {
            if (ex instanceof IOException || ex instanceof SecurityException) {
                ServiceStatusHolder.addWriteIOErrCnt();
                BrokerSrvStatsHolder.incDiskIOExcCnt();
            }
            logger.error("[File offsets] recover offset storage files failed!", ex);
            return false;
        }
    }

    public RetValue backupGroupOffsets(String backupPath) {
        String fileNameBase = backupPath + File.separator + OFFSET_FILENAME;
        return storeOffsetStgInfoToFile(this.groupOffsetInfo, fileNameBase);
    }

    public static RetValue storeOffsetStgInfoToFile(GroupOffsetStgInfo offsetStgInfo, String fileNameBase) {
        RetValue result = new RetValue();
        String configStr;
        try {
            offsetStgInfo.setLstStoreTime();
            configStr = GSON.toJson(offsetStgInfo, GroupOffsetStgInfo.class);
        } catch (Throwable ex) {
            logger.error("[File offsets] serial offset configure to json failure", ex);
            result.setFailResult("serial offset configure to json failure: " + ex.getMessage());
            return result;
        }
        File tmpFile = new File(fileNameBase + OFFSET_FILENAME_SUFFIX_TMP);
        if (!storeConfigToFile(configStr, tmpFile)) {
            logger.error("[File offsets] store json config to file {} failure", tmpFile.getAbsoluteFile());
            result.setFailResult("store json config failure to file: " + tmpFile.getAbsoluteFile());
            return result;
        }
        String readConfigStr = getConfigFromFile(tmpFile);
        if (!configStr.equals(readConfigStr)) {
            result.setFailResult("read stored offset json not equal!");
            return result;
        }
        File midFile = new File(fileNameBase + OFFSET_FILENAME_SUFFIX_MID);
        File dstFile = new File(fileNameBase + OFFSET_FILENAME_SUFFIX_FORMAL);
        File oldFile = new File(fileNameBase + OFFSET_FILENAME_SUFFIX_OLD);
        try {
            FileUtils.moveFile(tmpFile, midFile);
            if (!midFile.exists()) {
                throw new Exception("Mid File " + midFile + " not found!");
            }
            if (dstFile.exists()) {
                FileUtils.moveFile(dstFile, oldFile);
                if (!oldFile.exists()) {
                    throw new Exception("Backup File " + oldFile + " not found!");
                }
            }
            FileUtils.moveFile(midFile, dstFile);
            if (!dstFile.exists()) {
                throw new Exception("Formal File " + dstFile + " not found!");
            }
            if (oldFile.exists()) {
                if (!oldFile.delete()) {
                    throw new Exception("Backup file " + oldFile + " delete failed!");
                }
            }
            result.setSuccResult();
        } catch (Throwable ex) {
            if (ex instanceof IOException || ex instanceof SecurityException) {
                ServiceStatusHolder.addWriteIOErrCnt();
                BrokerSrvStatsHolder.incDiskIOExcCnt();
            }
            logger.error("[File offsets] exception thrown while adjust file name", ex);
            result.setFailResult("Exception while store to file: " + ex.getMessage());
        }
        return result;
    }

    public static GroupOffsetStgInfo getOffsetStgInfoFromConfig(String metaJsonStr) {
        try {
            return GSON.fromJson(metaJsonStr, GroupOffsetStgInfo.class);
        } catch (Throwable ex) {
            logger.error("[File offsets] parse offsets json failure, contents={}", metaJsonStr, ex);
            return null;
        }
    }

    public static boolean storeConfigToFile(String metaJsonStr, File tgtFile) {
        boolean isSuccess = false;
        try {
            FileUtils.writeStringToFile(tgtFile, metaJsonStr, StandardCharsets.UTF_8);
            isSuccess = true;
        } catch (Throwable ex) {
            if (ex instanceof IOException) {
                ServiceStatusHolder.addWriteIOErrCnt();
                BrokerSrvStatsHolder.incDiskIOExcCnt();
            }
            logger.error("[File offsets] exception thrown while writing to file {}",
                    tgtFile.getAbsoluteFile(), ex);
        }
        return isSuccess;
    }

    public static String getConfigFromFile(File tgtFile) {
        try {
            return FileUtils.readFileToString(tgtFile, StandardCharsets.UTF_8);
        } catch (Throwable ex) {
            if (ex instanceof IOException) {
                ServiceStatusHolder.addReadIOErrCnt();
                BrokerSrvStatsHolder.incDiskIOExcCnt();
            }
            logger.error("[File offsets] exception thrown while load from file {}",
                    tgtFile.getAbsoluteFile(), ex);
            return null;
        }
    }
}
