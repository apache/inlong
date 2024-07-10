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

package org.apache.inlong.sort.standalone.sink.pulsar;

import org.apache.inlong.common.pojo.sort.node.PulsarNodeConfig;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.config.holder.CommonPropertiesHolder;
import org.apache.inlong.sort.standalone.config.pojo.CacheClusterConfig;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import org.apache.flume.Transaction;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

/**
 * 
 * PulsarProducerFederation
 */
public class PulsarProducerFederation {

    public static final Logger LOG = InlongLoggerFactory.getLogger(PulsarProducerFederation.class);

    private final String workerName;
    private final PulsarFederationSinkContext context;
    private Timer reloadTimer;
    private PulsarNodeConfig nodeConfig;
    private CacheClusterConfig cacheClusterConfig;
    private PulsarProducerCluster cluster;
    private PulsarProducerCluster deleteCluster;

    /**
     * Constructor
     * 
     * @param workerName
     * @param context
     */
    public PulsarProducerFederation(String workerName, PulsarFederationSinkContext context) {
        this.workerName = workerName;
        this.context = context;
    }

    /**
     * start
     */
    public void start() {
        try {
            this.reload();
            this.setReloadTimer();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * close
     */
    public void close() {
        try {
            this.reloadTimer.cancel();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        cluster.stop();
    }

    /**
     * setReloadTimer
     */
    private void setReloadTimer() {
        reloadTimer = new Timer(true);
        TimerTask task = new TimerTask() {

            public void run() {
                reload();
            }
        };
        reloadTimer.schedule(task, new Date(System.currentTimeMillis() + context.getReloadInterval()),
                context.getReloadInterval());
    }

    /**
     * reload
     */
    public void reload() {
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

    private void reloadByNodeConfig() {
        try {
            if (nodeConfig != null && context.getNodeConfig().getVersion() <= nodeConfig.getVersion()) {
                return;
            }
            this.nodeConfig = context.getNodeConfig();
            PulsarProducerCluster updateCluster =
                    new PulsarProducerCluster(workerName, cacheClusterConfig, nodeConfig, context);
            updateCluster.start();
            this.deleteCluster = cluster;
            this.cluster = updateCluster;
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void reloadByCacheClusterConfig() {
        try {
            if (cacheClusterConfig != null && !cacheClusterConfig.equals(context.getCacheClusterConfig())) {
                return;
            }
            this.cacheClusterConfig = context.getCacheClusterConfig();
            PulsarProducerCluster updateCluster =
                    new PulsarProducerCluster(workerName, cacheClusterConfig, nodeConfig, context);
            updateCluster.start();
            this.deleteCluster = cluster;
            this.cluster = updateCluster;
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public boolean send(ProfileEvent profileEvent, Transaction tx) throws IOException {
        return cluster.send(profileEvent, tx);
    }
}
