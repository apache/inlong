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

package org.apache.inlong.audit.service.selector.impl;

import org.apache.inlong.audit.service.config.ConfigConstants;
import org.apache.inlong.audit.service.config.Configuration;
import org.apache.inlong.audit.service.selector.api.Selector;
import org.apache.inlong.audit.service.selector.api.SelectorConfig;
import org.apache.inlong.audit.service.selector.task.DBMonitorTask;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Elector Impl
 */
public class SelectorImpl extends Selector {

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectorImpl.class);
    private final SelectorConfig selectorConfig;
    private final ExecutorService fixedThreadPool;
    private boolean canElector = true;
    private final DBDataSource dbDataSource;
    private long sleepTime = 0L;
    private boolean running = true;

    public SelectorImpl(SelectorConfig selectorConfig) {
        this.selectorConfig = selectorConfig;
        this.dbDataSource = new DBDataSource(selectorConfig);
        fixedThreadPool = Executors.newFixedThreadPool(Configuration.getInstance().get(
                ConfigConstants.KEY_SELECTOR_THREAD_POOL_SIZE,
                ConfigConstants.DEFAULT_SELECTOR_THREAD_POOL_SIZE));
    }

    /**
     * init
     *
     * @throws Exception
     */
    public void init() throws Exception {
        try {
            LOGGER.info("Init selector impl...");

            dbDataSource.init(true);

            fixedThreadPool.execute(new ElectorWorkerThread());

            fixedThreadPool.execute(new DBMonitorTask(selectorConfig, dbDataSource));
        } catch (Exception exception) {
            LOGGER.error("Failed to init selector", exception);
        }
    }

    /**
     * Judge where is leader
     *
     * @return
     */
    public boolean isLeader() {
        return this.isLeader;
    }

    /**
     * Release leader
     */
    public void releaseLeader() {
        if (this.isLeader)
            try {
                dbDataSource.releaseLeader();
            } catch (Exception exception) {
                LOGGER.error("Exception :{}", exception.getMessage());
            }

        try {
            TimeUnit.SECONDS.sleep(Configuration.getInstance().get(ConfigConstants.KEY_RELEASE_LEADER_INTERVAL,
                    ConfigConstants.DEFAULT_RELEASE_LEADER_INTERVAL));
        } catch (Exception exception) {
            LOGGER.error("Exception :{}", exception.getMessage());
        }
    }

    /**
     * Replace leader
     *
     * @param newLeaderId
     */
    public void replaceLeader(String newLeaderId) {
        sleepTime = (selectorConfig.getTryToBeLeaderInterval() * 2L);
        dbDataSource.replaceLeader(newLeaderId);
    }

    /**
     * Get leader
     *
     * @param serviceId
     * @return
     */
    public String getLeader(String serviceId) {
        return dbDataSource.getCurrentLeader();
    }

    /**
     * Judge where can be elector
     *
     * @param canElector
     */
    public void canSelect(boolean canElector) {
        this.canElector = canElector;
    }

    /**
     * Rebuild elector DBSource
     *
     * @return
     */
    public boolean rebuildSelectorDBSource() {
        canSelect(false);
        try {
            releaseLeader();
            dbDataSource.close();
            dbDataSource.init(false);
            canSelect(true);
        } catch (Exception exception) {
            LOGGER.error("Exception :{}", exception.getMessage());
            return false;
        }
        return true;
    }

    /**
     * close
     *
     * @return
     */
    public void close() {
        running = false;
        dbDataSource.close();
        fixedThreadPool.shutdown();
    }

    class ElectorWorkerThread implements Runnable {

        Random random;

        ElectorWorkerThread() {
            this.random = new Random();
        }

        public void run() {
            while (running) {
                if (canElector) {
                    dbDataSource.leaderSelector();
                }

                String leaderId = dbDataSource.getCurrentLeader();
                if (StringUtils.isNotEmpty(leaderId)) {
                    if (selectorConfig.getLeaderId().equals(leaderId)) {
                        if (!isLeader
                                && selectorConfig.getSelectorChangeListener() != null) {
                            selectorConfig.getSelectorChangeListener().leaderChanged(true);
                        }

                        isLeader = true;
                        sleepTime = selectorConfig.getTryToBeLeaderInterval();
                    } else {
                        if (isLeader
                                && selectorConfig.getSelectorChangeListener() != null) {
                            selectorConfig.getSelectorChangeListener().leaderChanged(false);
                        }

                        isLeader = false;
                        sleepTime = selectorConfig.getTryToBeLeaderInterval()
                                + random.nextInt(ConfigConstants.RANDOM_BOUND);
                    }
                }

                try {
                    TimeUnit.SECONDS.sleep(sleepTime);
                } catch (Exception exception) {
                    LOGGER.error("Exception :{}", exception.getMessage());
                }
            }
        }
    }
}