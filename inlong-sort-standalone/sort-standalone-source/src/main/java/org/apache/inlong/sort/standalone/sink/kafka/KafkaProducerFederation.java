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

package org.apache.inlong.sort.standalone.sink.kafka;

import org.apache.inlong.common.pojo.sort.node.KafkaNodeConfig;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.pojo.CacheClusterConfig;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * KafkaProducerFederation.
 */
public class KafkaProducerFederation implements Runnable {

    private static final Logger LOG = InlongLoggerFactory.getLogger(KafkaProducerFederation.class);
    private static final int CORE_POOL_SIZE = 1;

    private final String workerName;
    private final KafkaFederationSinkContext context;
    private ScheduledExecutorService pool;
    private long reloadInterval;
    private KafkaNodeConfig nodeConfig;
    private KafkaProducerCluster cluster;
    private KafkaProducerCluster deleteCluster;
    private CacheClusterConfig cacheClusterConfig;

    public KafkaProducerFederation(String workerName, KafkaFederationSinkContext context) {
        this.workerName = Preconditions.checkNotNull(workerName);
        this.context = Preconditions.checkNotNull(context);
        this.reloadInterval = context.getReloadInterval();
    }

    public void close() {
        try {
            this.pool.shutdownNow();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        cluster.stop();
    }

    /** start */
    public void start() {
        try {
            this.reload();
            this.initReloadExecutor();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void run() {
        this.reload();
    }

    private void reload() {
        try {
            if (deleteCluster != null) {
                deleteCluster.stop();
                deleteCluster = null;
            }
        } catch (Exception e) {
            LOG.error("failed to close delete cluster, ex={}", e.getMessage(), e);
        }

        if (CommonPropertiesHolder.useUnifiedConfiguration()) {
            reloadByNodeConfig();
        } else {
            reloadByCacheClusterConfig();
        }

    }

    private void reloadByCacheClusterConfig() {
        try {
            if (cacheClusterConfig != null && !cacheClusterConfig.equals(context.getCacheClusterConfig())) {
                return;
            }
            this.cacheClusterConfig = context.getCacheClusterConfig();
            KafkaProducerCluster updateCluster = new KafkaProducerCluster(workerName, cacheClusterConfig, nodeConfig,
                    context);
            updateCluster.start();
            this.deleteCluster = cluster;
            this.cluster = updateCluster;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

    }

    private void reloadByNodeConfig() {
        try {
            if (nodeConfig != null && context.getNodeConfig().getVersion() <= nodeConfig.getVersion()) {
                return;
            }
            this.nodeConfig = context.getNodeConfig();
            KafkaProducerCluster updateCluster = new KafkaProducerCluster(workerName, cacheClusterConfig, nodeConfig,
                    context);
            updateCluster.start();
            this.deleteCluster = cluster;
            this.cluster = updateCluster;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public boolean send(KafkaTransaction ktx, ProfileEvent profileEvent, KafkaIdConfig idConfig) throws IOException {
        return cluster.send(ktx, profileEvent, idConfig);
    }

    /** Init ScheduledExecutorService with fix reload rate {@link #reloadInterval}. */
    private void initReloadExecutor() {
        this.pool = Executors.newScheduledThreadPool(CORE_POOL_SIZE);
        pool.scheduleAtFixedRate(this, reloadInterval, reloadInterval, TimeUnit.SECONDS);
    }
}
