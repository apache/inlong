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

package org.apache.inlong.dataproxy.config.holder;

import org.apache.inlong.dataproxy.config.CommonConfigHolder;
import org.apache.inlong.dataproxy.config.ConfigHolder;
import org.apache.inlong.dataproxy.config.ConfigManager;
import org.apache.inlong.dataproxy.config.pojo.CacheClusterConfig;
import org.apache.inlong.dataproxy.config.pojo.CacheType;
import org.apache.inlong.dataproxy.config.pojo.IdTopicConfig;
import org.apache.inlong.dataproxy.config.pojo.InLongMetaConfig;
import org.apache.inlong.sdk.commons.protocol.InlongId;

import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Json to object
 */
public class MetaConfigHolder extends ConfigHolder {

    private static final String metaConfigFileName = "metadata.json";
    private static final int MAX_ALLOWED_JSON_FILE_SIZE = 300 * 1024 * 1024;
    private static final Logger LOG = LoggerFactory.getLogger(MetaConfigHolder.class);
    private static final Gson GSON = new Gson();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    // meta data
    private String dataMd5 = "";
    private String dataStr = "";
    private final AtomicLong lastUpdVersion = new AtomicLong(0);
    private String tmpDataMd5 = "";
    private final AtomicLong lastSyncVersion = new AtomicLong(0);
    // cached data
    private final AtomicInteger clusterType = new AtomicInteger(CacheType.N.getId());
    private final ConcurrentHashMap<String, CacheClusterConfig> mqClusterMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, IdTopicConfig> id2TopicSrcMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, IdTopicConfig> id2TopicSinkMap = new ConcurrentHashMap<>();

    public MetaConfigHolder() {
        super(metaConfigFileName);
    }

    /**
     * get source topic by groupId and streamId
     */
    public String getSrcBaseTopicName(String groupId, String streamId) {
        IdTopicConfig idTopicConfig = getSrcIdTopicConfig(groupId, streamId);
        if (idTopicConfig == null) {
            return null;
        }
        return idTopicConfig.getTopicName();
    }

    public IdTopicConfig getSrcIdTopicConfig(String groupId, String streamId) {
        IdTopicConfig idTopicConfig = null;
        if (StringUtils.isNotEmpty(groupId) && !id2TopicSrcMap.isEmpty()) {
            idTopicConfig = id2TopicSrcMap.get(InlongId.generateUid(groupId, streamId));
            if (idTopicConfig == null) {
                idTopicConfig = id2TopicSrcMap.get(groupId);
            }
        }
        return idTopicConfig;
    }

    /**
     * get topic by groupId and streamId
     */
    public String getSourceTopicName(String groupId, String streamId) {
        String topic = null;
        if (StringUtils.isNotEmpty(groupId) && !id2TopicSrcMap.isEmpty()) {
            IdTopicConfig idTopicConfig = id2TopicSrcMap.get(InlongId.generateUid(groupId, streamId));
            if (idTopicConfig == null) {
                idTopicConfig = id2TopicSrcMap.get(groupId);
            }
            if (idTopicConfig != null) {
                topic = idTopicConfig.getTopicName();
            }
        }
        return topic;
    }

    public IdTopicConfig getSinkIdTopicConfig(String groupId, String streamId) {
        IdTopicConfig idTopicConfig = null;
        if (StringUtils.isNotEmpty(groupId) && !id2TopicSinkMap.isEmpty()) {
            idTopicConfig = id2TopicSinkMap.get(InlongId.generateUid(groupId, streamId));
            if (idTopicConfig == null) {
                idTopicConfig = id2TopicSinkMap.get(groupId);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get Sink Topic Config by groupId = {}, streamId = {}, IdTopicConfig = {}",
                    groupId, streamId, idTopicConfig);
        }
        return idTopicConfig;
    }

    public String getConfigMd5() {
        if (this.lastSyncVersion.get() > this.lastUpdVersion.get()) {
            return tmpDataMd5;
        } else {
            return dataMd5;
        }
    }

    public boolean updateConfigMap(InLongMetaConfig metaConfig) {
        String inDataJsonStr;
        // check cache data
        synchronized (this.lastSyncVersion) {
            if (this.lastSyncVersion.get() > this.lastUpdVersion.get()) {
                if (tmpDataMd5.equals(metaConfig.getMd5())) {
                    return false;
                }
                LOG.info("Update metadata: NOT UPDATE, {} is loading, but wast over {} ms",
                        getFileName(), System.currentTimeMillis() - this.lastSyncVersion.get());
                return false;
            } else {
                if (dataMd5.equals(metaConfig.getMd5())) {
                    return false;
                }
            }
            InLongMetaConfig newMetaConfig = buildMixedMetaConfig(metaConfig);
            try {
                inDataJsonStr = GSON.toJson(newMetaConfig);
            } catch (Throwable e) {
                LOG.error("Update metadata: failure to serial meta config to json", e);
                return false;
            }
            return storeConfigToFile(inDataJsonStr, newMetaConfig);
        }
    }

    private InLongMetaConfig buildMixedMetaConfig(InLongMetaConfig metaConfig) {
        // process and check cluster info
        Map<String, CacheClusterConfig> newClusterConfigMap =
                new HashMap<>(metaConfig.getClusterConfigMap().size());
        newClusterConfigMap.putAll(metaConfig.getClusterConfigMap());
        // process id2topic info
        Map<String, IdTopicConfig> newIdTopicConfigMap =
                new HashMap<>(metaConfig.getIdTopicConfigMap().size());
        newIdTopicConfigMap.putAll(metaConfig.getIdTopicConfigMap());
        return new InLongMetaConfig(metaConfig.getMd5(),
                metaConfig.getMqType(), newClusterConfigMap, newIdTopicConfigMap);
    }

    public List<CacheClusterConfig> forkCachedCLusterConfig() {
        List<CacheClusterConfig> result = new ArrayList<>();
        if (mqClusterMap.isEmpty()) {
            return result;
        }
        for (Map.Entry<String, CacheClusterConfig> entry : mqClusterMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            CacheClusterConfig config = new CacheClusterConfig();
            config.setClusterName(entry.getValue().getClusterName());
            config.setToken(entry.getValue().getToken());
            config.getParams().putAll(entry.getValue().getParams());
            result.add(config);
        }
        return result;
    }

    public Set<String> getAllTopicName() {
        Set<String> result = new HashSet<>();
        // add default topics first
        if (CommonConfigHolder.getInstance().isEnableUnConfigTopicAccept()) {
            result.addAll(CommonConfigHolder.getInstance().getDefTopics());
        }
        // add configured topics
        for (IdTopicConfig topicConfig : id2TopicSrcMap.values()) {
            if (topicConfig == null) {
                continue;
            }
            result.add(topicConfig.getTopicName());
        }
        return result;
    }

    @Override
    protected boolean loadFromFileToHolder() {
        // check meta update setting
        if (!CommonConfigHolder.getInstance().isEnableStartupUsingLocalMetaFile()
                && !ConfigManager.handshakeManagerOk.get()) {
            LOG.warn("Load metadata: StartupUsingLocalMetaFile is false, don't obtain metadata from {}"
                    + " before handshake with Manager", getFileName());
            return false;
        }
        String jsonString = "";
        InLongMetaConfig metaConfig;
        readWriteLock.writeLock().lock();
        try {
            jsonString = loadConfigFromFile();
            if (StringUtils.isBlank(jsonString)) {
                LOG.error("Load metadata: NOT LOADED, changed but empty records, file:{}", getFileName());
                return true;
            }
            try {
                metaConfig = GSON.fromJson(jsonString, InLongMetaConfig.class);
            } catch (Throwable e) {
                LOG.error("Load metadata: NOT LOADED, parse json config failure, file:{}", getFileName(), e);
                return true;
            }
            // check required fields
            ImmutablePair<Boolean, String> paramChkResult = validRequiredFields(metaConfig);
            if (!paramChkResult.getLeft()) {
                LOG.error("Load metadata: NOT LOADED, {}, file:{}",
                        paramChkResult.getRight(), getFileName());
                return true;
            }
            // update cached data
            replaceCacheConfig(metaConfig.getMqType(),
                    metaConfig.getClusterConfigMap(), metaConfig.getIdTopicConfigMap());
            this.dataMd5 = metaConfig.getMd5();
            this.dataStr = jsonString;
            LOG.info("Load metadata: LOADED success, from {}!", getFileName());
            return true;
        } catch (Throwable e) {
            LOG.error("Load metadata: NOT LOADED, load from {} throw exception", getFileName(), e);
            return false;
        } finally {
            if (this.lastSyncVersion.get() == 0) {
                this.lastUpdVersion.set(System.currentTimeMillis());
                this.lastSyncVersion.compareAndSet(0, this.lastUpdVersion.get());
            } else {
                this.lastUpdVersion.set(this.lastSyncVersion.get());
            }
            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * store meta config to file
     *
     * @param metaJsonStr meta info string
     * @param metaConfig  meta info object
     *
     * @return  store result
     */
    private boolean storeConfigToFile(String metaJsonStr, InLongMetaConfig metaConfig) {
        boolean isSuccess = false;
        String filePath = getFilePath();
        if (StringUtils.isBlank(filePath)) {
            LOG.error("Store metadata: error in writing file {} as the file path is null.", getFileName());
            return isSuccess;
        }
        readWriteLock.writeLock().lock();
        try {
            File sourceFile = new File(filePath);
            File targetFile = new File(getNextBackupFileName());
            File tmpNewFile = new File(getFileName() + ".tmp");

            if (sourceFile.exists()) {
                FileUtils.copyFile(sourceFile, targetFile);
            }
            FileUtils.writeStringToFile(tmpNewFile, metaJsonStr, StandardCharsets.UTF_8);
            FileUtils.copyFile(tmpNewFile, sourceFile);
            tmpNewFile.delete();
            tmpDataMd5 = metaConfig.getMd5();
            lastSyncVersion.set(System.currentTimeMillis());
            isSuccess = true;
            setFileChanged();
        } catch (Throwable ex) {
            LOG.error("Store metadata: exception thrown while writing to file {}", getFileName(), ex);
        } finally {
            readWriteLock.writeLock().unlock();
        }
        return isSuccess;
    }

    /**
     * update locally cached configuration with input information
     *
     * @param cacheType  mq cluster type
     * @param clusterConfigMap  mq cluster config
     * @param idTopicConfigMap  id to topic config
     */
    private void replaceCacheConfig(CacheType cacheType,
            Map<String, CacheClusterConfig> clusterConfigMap,
            Map<String, IdTopicConfig> idTopicConfigMap) {
        this.clusterType.getAndSet(cacheType.getId());
        // remove deleted id2topic config
        Set<String> tmpRmvKeys = new HashSet<>();
        for (Map.Entry<String, IdTopicConfig> entry : id2TopicSrcMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (!idTopicConfigMap.containsKey(entry.getKey())) {
                tmpRmvKeys.add(entry.getKey());
            }
        }
        for (String key : tmpRmvKeys) {
            id2TopicSrcMap.remove(key);
        }
        // add new id2topic source config
        id2TopicSrcMap.putAll(idTopicConfigMap);
        // add new id2topic sink config
        id2TopicSinkMap.putAll(idTopicConfigMap);
        // remove deleted cluster config
        tmpRmvKeys.clear();
        for (Map.Entry<String, CacheClusterConfig> entry : mqClusterMap.entrySet()) {
            if (entry == null || entry.getKey() == null || entry.getValue() == null) {
                continue;
            }
            if (!clusterConfigMap.containsKey(entry.getKey())) {
                tmpRmvKeys.add(entry.getKey());
            }
        }
        for (String key : tmpRmvKeys) {
            mqClusterMap.remove(key);
        }
        // add new mq cluster config
        mqClusterMap.putAll(clusterConfigMap);
    }

    /**
     * load configure from holder
     *
     * @return the configure info
     */
    private String loadConfigFromFile() {
        String result = "";
        if (StringUtils.isBlank(getFileName())) {
            LOG.error("Load metadata: fail to load json {} as the file name is null.", getFileName());
            return result;
        }
        InputStream inStream = null;
        try {
            URL url = getClass().getClassLoader().getResource(getFileName());
            inStream = url != null ? url.openStream() : null;
            if (inStream == null) {
                LOG.error("Load metadata: fail to load json {} as the input stream is null", getFileName());
                return result;
            }
            int size = inStream.available();
            if (size > MAX_ALLOWED_JSON_FILE_SIZE) {
                LOG.error("Load metadata: fail to load json {} as the content size({}) over max allowed size({})",
                        getFileName(), size, MAX_ALLOWED_JSON_FILE_SIZE);
                return result;
            }
            byte[] buffer = new byte[size];
            inStream.read(buffer);
            result = new String(buffer, StandardCharsets.UTF_8);
        } catch (Throwable e) {
            LOG.error("Load metadata: exception thrown while load from file {}", getFileName(), e);
        } finally {
            if (null != inStream) {
                try {
                    inStream.close();
                } catch (IOException e) {
                    LOG.error("Load metadata: fail in inStream.close for file {}", getFileName(), e);
                }
            }
        }
        return result;
    }

    /**
     * check required fields status
     *
     * @param metaConfig  response from Manager
     *
     * @return   check result
     */
    public ImmutablePair<Boolean, String> validRequiredFields(InLongMetaConfig metaConfig) {
        if (metaConfig == null) {
            return ImmutablePair.of(false, "metaConfig object is null");
        } else if (metaConfig.getMd5() == null) {
            return ImmutablePair.of(false, "metaConfig.md5 field is null");
        } else if (metaConfig.getMqType() == null) {
            return ImmutablePair.of(false, "metaConfig.mqType field is null");
        } else if (metaConfig.getMqType() == CacheType.N) {
            return ImmutablePair.of(false, "metaConfig.mqType value is CacheType.N");
        } else if (metaConfig.getClusterConfigMap() == null) {
            return ImmutablePair.of(false, "metaConfig.clusterConfigMap field is null");
        } else if (metaConfig.getClusterConfigMap().isEmpty()) {
            return ImmutablePair.of(false, "metaConfig.clusterConfigMap field is empty");
        } else if (metaConfig.getIdTopicConfigMap() == null) {
            return ImmutablePair.of(false, "metaConfig.idTopicConfigMap field is null");
        } else if (metaConfig.getIdTopicConfigMap().isEmpty()) {
            return ImmutablePair.of(false, "metaConfig.idTopicConfigMap is empty");
        }
        return ImmutablePair.of(true, "ok");
    }

}
