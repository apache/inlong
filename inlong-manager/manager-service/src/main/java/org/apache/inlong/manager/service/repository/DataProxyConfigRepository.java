/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.service.repository;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.inlong.manager.common.pojo.dataproxy.CacheClusterObject;
import org.apache.inlong.manager.common.pojo.dataproxy.CacheClusterSetObject;
import org.apache.inlong.manager.common.pojo.dataproxy.CacheTopicObject;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyCluster;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyClusterSet;
import org.apache.inlong.manager.common.pojo.dataproxy.DataProxyConfigResponse;
import org.apache.inlong.manager.common.pojo.dataproxy.InLongIdObject;
import org.apache.inlong.manager.common.pojo.dataproxy.ProxyChannel;
import org.apache.inlong.manager.common.pojo.dataproxy.ProxyClusterObject;
import org.apache.inlong.manager.common.pojo.dataproxy.ProxySink;
import org.apache.inlong.manager.common.pojo.dataproxy.ProxySource;
import org.apache.inlong.manager.dao.entity.CacheCluster;
import org.apache.inlong.manager.dao.entity.CacheClusterExt;
import org.apache.inlong.manager.dao.entity.CacheTopic;
import org.apache.inlong.manager.dao.entity.ClusterSet;
import org.apache.inlong.manager.dao.entity.FlumeChannel;
import org.apache.inlong.manager.dao.entity.FlumeChannelExt;
import org.apache.inlong.manager.dao.entity.FlumeSink;
import org.apache.inlong.manager.dao.entity.FlumeSinkExt;
import org.apache.inlong.manager.dao.entity.FlumeSource;
import org.apache.inlong.manager.dao.entity.FlumeSourceExt;
import org.apache.inlong.manager.dao.entity.InLongId;
import org.apache.inlong.manager.dao.entity.ProxyCluster;
import org.apache.inlong.manager.dao.entity.ProxyClusterToCacheCluster;
import org.apache.inlong.manager.dao.mapper.ClusterSetMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.google.common.base.Splitter;
import com.google.gson.Gson;

/**
 * DataProxyConfigRepository
 */
@Repository(value = "dataProxyConfigRepository")
public class DataProxyConfigRepository implements IRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataProxyConfigRepository.class);
    public static final Splitter.MapSplitter MAP_SPLITTER = Splitter.on(SEPARATOR).trimResults()
            .withKeyValueSeparator(KEY_VALUE_SEPARATOR);
    @Autowired
    private ClusterSetMapper clusterSetMapper;
    // Map<setName, DataProxyClusterSet>
    private Map<String, DataProxyClusterSet> clusterSets = new HashMap<>();

    // Map<proxyClusterName, ProxyClusterObject>
    private Map<String, ProxyClusterObject> proxyClusterMap = new HashMap<>();
    // Map<cacheClusterName, CacheClusterObject>
    private Map<String, CacheClusterObject> cacheClusterMap = new HashMap<>();

    private long reloadInterval;
    private Timer reloadTimer;

    private Gson gson = new Gson();

    public DataProxyConfigRepository() {
        LOGGER.info("create repository for {}" + DataProxyConfigRepository.class.getSimpleName());
        try {
            this.reloadInterval = DEFAULT_HEARTBEAT_INTERVAL_MS;
            reload();
            setReloadTimer();
        } catch (Throwable t) {
            LOGGER.error(t.getMessage(), t);
        }
    }

    /**
     * reload
     */
    public void reload() {
        LOGGER.info("start to reload config.");
        List<ClusterSet> setList = clusterSetMapper.selectClusterSet();
        if (setList.size() == 0) {
            return;
        }

        Map<String, DataProxyClusterSet> newClusterSets = new HashMap<>();
        for (ClusterSet set : setList) {
            String setName = set.getSetName();
            DataProxyClusterSet setObj = new DataProxyClusterSet();
            setObj.setSetName(setName);
            setObj.getCacheClusterSet().setSetName(setName);
            setObj.getCacheClusterSet().setType(set.getMiddlewareType());
            newClusterSets.put(setName, setObj);
        }
        //
        this.proxyClusterMap.clear();
        this.cacheClusterMap.clear();
        //
        this.reloadCacheCluster(newClusterSets);
        //
        this.reloadCacheClusterExt(newClusterSets);
        //
        this.reloadCacheTopic(newClusterSets);
        //
        this.reloadProxyCluster(newClusterSets);
        //
        this.reloadFlumeChannel(newClusterSets);
        //
        this.reloadFlumeChannelExt(newClusterSets);
        //
        this.reloadFlumeSource(newClusterSets);
        //
        this.reloadFlumeSourceExt(newClusterSets);
        //
        this.reloadFlumeSink(newClusterSets);
        //
        this.reloadFlumeSinkExt(newClusterSets);
        // reload inlongid
        this.reloadInlongId(newClusterSets);
        //
        this.reloadProxy2Cache(newClusterSets);
        //
        this.generateClusterJson(newClusterSets);

        // replace
        this.clusterSets = newClusterSets;

        LOGGER.info("end to reload config.");
    }

    /**
     * setReloadTimer
     */
    private void setReloadTimer() {
        reloadTimer = new Timer(true);
        TimerTask task = new RepositoryTimerTask<DataProxyConfigRepository>(this);
        reloadTimer.schedule(task, new Date(System.currentTimeMillis() + reloadInterval), reloadInterval);
    }

    /**
     * get clusterSets
     * 
     * @return the clusterSets
     */
    public Map<String, DataProxyClusterSet> getClusterSets() {
        return clusterSets;
    }

    /**
     * 
     * reloadCacheCluster
     * 
     * @param newClusterSets
     */
    private void reloadCacheCluster(Map<String, DataProxyClusterSet> newClusterSets) {
        for (CacheCluster cacheCluster : clusterSetMapper.selectCacheCluster()) {
            //
            CacheClusterObject obj = new CacheClusterObject();
            obj.setName(cacheCluster.getClusterName());
            obj.setZone(cacheCluster.getZone());
            cacheClusterMap.put(obj.getName(), obj);
            //
            String setName = cacheCluster.getSetName();
            DataProxyClusterSet setObj = this.getOrCreateDataProxyClusterSet(newClusterSets, setName);
            setObj.getCacheClusterSet().getCacheClusters().add(obj);
        }
    }

    /**
     * getOrCreateDataProxyClusterSet
     * 
     * @param clusterSets
     * @param setName
     * @return
     */
    private DataProxyClusterSet getOrCreateDataProxyClusterSet(Map<String, DataProxyClusterSet> clusterSets,
            String setName) {
        DataProxyClusterSet setObj = clusterSets.get(setName);
        if (setObj == null) {
            setObj = new DataProxyClusterSet();
            setObj.setSetName(setName);
            clusterSets.put(setName, setObj);
        }
        return setObj;
    }

    /**
     * 
     * reloadCacheClusterExt
     * 
     * @param newClusterSets
     */
    private void reloadCacheClusterExt(Map<String, DataProxyClusterSet> newClusterSets) {
        for (CacheClusterExt ext : clusterSetMapper.selectCacheClusterExt()) {
            String clusterName = ext.getClusterName();
            CacheClusterObject cacheClusterObject = cacheClusterMap.get(clusterName);
            if (cacheClusterObject != null) {
                cacheClusterObject.getParams().put(ext.getKeyName(), ext.getKeyValue());
            }
        }
    }

    /**
     * 
     * reloadCacheTopic
     * 
     * @param newClusterSets
     */
    private void reloadCacheTopic(Map<String, DataProxyClusterSet> newClusterSets) {
        for (CacheTopic cacheTopic : clusterSetMapper.selectCacheTopic()) {
            //
            CacheTopicObject obj = new CacheTopicObject();
            obj.setTopic(cacheTopic.getTopicName());
            obj.setPartitionNum(cacheTopic.getPartitionNum());
            //
            String setName = cacheTopic.getSetName();
            DataProxyClusterSet setObj = this.getOrCreateDataProxyClusterSet(newClusterSets, setName);
            setObj.getCacheClusterSet().getTopics().add(obj);
        }
    }

    /**
     * 
     * reloadProxyCluster
     * 
     * @param newClusterSets
     */
    private void reloadProxyCluster(Map<String, DataProxyClusterSet> newClusterSets) {
        for (ProxyCluster proxyCluster : clusterSetMapper.selectProxyCluster()) {
            String setName = proxyCluster.getSetName();
            //
            ProxyClusterObject obj = new ProxyClusterObject();
            obj.setName(proxyCluster.getClusterName());
            obj.setSetName(setName);
            obj.setZone(proxyCluster.getZone());
            DataProxyClusterSet setObj = this.getOrCreateDataProxyClusterSet(newClusterSets, setName);
            setObj.getProxyClusterList().add(obj);
            this.proxyClusterMap.put(obj.getName(), obj);
        }
    }

    /**
     * 
     * reloadFlumeChannel
     * 
     * @param newClusterSets
     */
    private void reloadFlumeChannel(Map<String, DataProxyClusterSet> newClusterSets) {
        for (FlumeChannel flumeChannel : clusterSetMapper.selectFlumeChannel()) {
            //
            ProxyChannel obj = new ProxyChannel();
            obj.setName(flumeChannel.getChannelName());
            obj.setType(flumeChannel.getType());
            String setName = flumeChannel.getSetName();
            DataProxyClusterSet setObj = this.getOrCreateDataProxyClusterSet(newClusterSets, setName);
            setObj.getProxyChannelMap().put(obj.getName(), obj);
        }
    }

    /**
     * 
     * reloadFlumeChannelExt
     * 
     * @param newClusterSets
     */
    private void reloadFlumeChannelExt(Map<String, DataProxyClusterSet> newClusterSets) {
        for (FlumeChannelExt ext : clusterSetMapper.selectFlumeChannelExt()) {
            String setName = ext.getSetName();
            DataProxyClusterSet setObj = this.getOrCreateDataProxyClusterSet(newClusterSets, setName);
            //
            ProxyChannel obj = setObj.getProxyChannelMap().get(ext.getParentName());
            if (obj != null) {
                obj.getParams().put(ext.getKeyName(), ext.getKeyValue());
            }
        }
    }

    /**
     * 
     * reloadFlumeSource
     * 
     * @param newClusterSets
     */
    private void reloadFlumeSource(Map<String, DataProxyClusterSet> newClusterSets) {
        for (FlumeSource flumeSource : clusterSetMapper.selectFlumeSource()) {
            //
            ProxySource obj = new ProxySource();
            obj.setName(flumeSource.getSourceName());
            obj.setSelectorType(flumeSource.getSelectorType());
            obj.setType(flumeSource.getType());
            //
            String channels = flumeSource.getChannels();
            obj.getChannels().addAll(Arrays.asList(channels.split("\\s+")));
            //
            String setName = flumeSource.getSetName();
            DataProxyClusterSet setObj = this.getOrCreateDataProxyClusterSet(newClusterSets, setName);
            setObj.getProxySourceMap().put(obj.getName(), obj);
        }
    }

    /**
     * 
     * reloadFlumeSourceExt
     * 
     * @param newClusterSets
     */
    private void reloadFlumeSourceExt(Map<String, DataProxyClusterSet> newClusterSets) {
        for (FlumeSourceExt ext : clusterSetMapper.selectFlumeSourceExt()) {
            String setName = ext.getSetName();
            DataProxyClusterSet setObj = this.getOrCreateDataProxyClusterSet(newClusterSets, setName);
            //
            ProxySource obj = setObj.getProxySourceMap().get(ext.getParentName());
            if (obj != null) {
                obj.getParams().put(ext.getKeyName(), ext.getKeyValue());
            }
        }
    }

    /**
     * 
     * reloadFlumeSink
     * 
     * @param newClusterSets
     */
    private void reloadFlumeSink(Map<String, DataProxyClusterSet> newClusterSets) {
        for (FlumeSink flumeSink : clusterSetMapper.selectFlumeSink()) {
            //
            ProxySink obj = new ProxySink();
            obj.setName(flumeSink.getSinkName());
            obj.setType(flumeSink.getType());
            obj.setChannel(flumeSink.getChannel());
            //
            String setName = flumeSink.getSetName();
            DataProxyClusterSet setObj = this.getOrCreateDataProxyClusterSet(newClusterSets, setName);
            setObj.getProxySinkMap().put(obj.getName(), obj);
        }
    }

    /**
     * 
     * reloadFlumeSinkExt
     * 
     * @param newClusterSets
     */
    private void reloadFlumeSinkExt(Map<String, DataProxyClusterSet> newClusterSets) {
        for (FlumeSinkExt ext : clusterSetMapper.selectFlumeSinkExt()) {
            String setName = ext.getSetName();
            DataProxyClusterSet setObj = this.getOrCreateDataProxyClusterSet(newClusterSets, setName);
            //
            ProxySink obj = setObj.getProxySinkMap().get(ext.getParentName());
            if (obj != null) {
                obj.getParams().put(ext.getKeyName(), ext.getKeyValue());
            }
        }
    }

    /**
     * 
     * reloadInlongId
     * 
     * @param newClusterSets
     */
    private void reloadInlongId(Map<String, DataProxyClusterSet> newClusterSets) {
        for (InLongId inlongId : clusterSetMapper.selectInlongId()) {
            //
            InLongIdObject obj = new InLongIdObject();
            obj.setInlongId(inlongId.getInlongId());
            obj.setTopic(inlongId.getTopic());
            if (inlongId.getParams() != null) {
                Map<String, String> params = MAP_SPLITTER.split(inlongId.getParams());
                obj.getParams().putAll(params);
            }
            String setName = inlongId.getSetName();
            DataProxyClusterSet setObj = this.getOrCreateDataProxyClusterSet(newClusterSets, setName);
            setObj.getInlongIds().add(obj);
        }
    }

    /**
     * 
     * reloadInlongId
     * 
     * @param newClusterSets
     */
    private void reloadProxy2Cache(Map<String, DataProxyClusterSet> newClusterSets) {
        for (ProxyClusterToCacheCluster proxy2Cache : clusterSetMapper.selectProxyClusterToCacheCluster()) {
            String proxyClusterName = proxy2Cache.getProxyClusterName();
            String cacheClusterName = proxy2Cache.getCacheClusterName();
            ProxyClusterObject proxyObj = this.proxyClusterMap.get(proxyClusterName);
            if (proxyObj == null) {
                continue;
            }
            CacheClusterObject cacheObj = this.cacheClusterMap.get(cacheClusterName);
            if (cacheObj == null) {
                continue;
            }
            //
            String setName = proxyObj.getSetName();
            DataProxyClusterSet setObj = this.getOrCreateDataProxyClusterSet(newClusterSets, setName);
            //
            setObj.addProxy2Cache(proxyClusterName, cacheClusterName);
        }
    }

    /**
     * 
     * generateClusterJson
     * 
     * @param newClusterSets
     */
    private void generateClusterJson(Map<String, DataProxyClusterSet> newClusterSets) {
        for (Entry<String, DataProxyClusterSet> entry : newClusterSets.entrySet()) {
            for (ProxyClusterObject proxyObj : entry.getValue().getProxyClusterList()) {
                // proxy
                DataProxyCluster clusterObj = new DataProxyCluster();
                clusterObj.setProxyCluster(proxyObj);
                // cache
                CacheClusterSetObject allCacheCluster = entry.getValue().getCacheClusterSet();
                CacheClusterSetObject proxyCacheClusterSet = clusterObj.getCacheClusterSet();
                proxyCacheClusterSet.setSetName(allCacheCluster.getSetName());
                proxyCacheClusterSet.setType(allCacheCluster.getType());
                proxyCacheClusterSet.setTopics(allCacheCluster.getTopics());
                // cacheCluster
                Set<String> cacheClusterNameSet = entry.getValue().getProxy2Cache().get(proxyObj.getName());
                if (cacheClusterNameSet != null) {
                    for (String cacheClusterName : cacheClusterNameSet) {
                        CacheClusterObject cacheObj = this.cacheClusterMap.get(cacheClusterName);
                        if (cacheObj == null) {
                            continue;
                        }
                        proxyCacheClusterSet.getCacheClusters().add(cacheObj);
                    }
                }
                //
                String jsonDataProxyCluster = gson.toJson(clusterObj);
                String md5 = DigestUtils.md5Hex(jsonDataProxyCluster);
                DataProxyConfigResponse response = new DataProxyConfigResponse();
                response.setResult(true);
                response.setErrCode(DataProxyConfigResponse.SUCC);
                response.setMd5(md5);
                response.setData(clusterObj);
                String jsonResponse = gson.toJson(clusterObj);
                entry.getValue().getProxyConfigJson().put(proxyObj.getName(), jsonResponse);
                entry.getValue().getMd5Map().put(proxyObj.getName(), md5);
                entry.getValue().setDefaultConfigJson(jsonResponse);
            }
        }
    }

    /**
     * 
     * getDataProxyClusterSet
     * 
     * @param setName
     * @return
     */
    public DataProxyClusterSet getDataProxyClusterSet(String setName) {
        DataProxyClusterSet setObj = this.clusterSets.get(setName);
        return setObj;
    }
}
