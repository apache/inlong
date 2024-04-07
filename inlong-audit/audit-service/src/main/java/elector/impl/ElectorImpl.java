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

package elector.impl;

import elector.api.Elector;
import elector.api.ElectorConfig;
import elector.task.DBMonitorTask;
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
public class ElectorImpl extends Elector {

    private static final Logger logger = LoggerFactory.getLogger(ElectorImpl.class);
    private final ElectorConfig electorConfig;
    private ExecutorService fixedThreadPool = Executors.newFixedThreadPool(3);
    private boolean canElector = true;
    private DBDataSource dbDataSource;
    private long sleepTime = 0L;

    public ElectorImpl(ElectorConfig electorConfig) {
        this.electorConfig = electorConfig;
        this.dbDataSource = new DBDataSource(electorConfig);
    }

    /**
     * init
     *
     * @throws Exception
     */
    public void init() throws Exception {
        try {
            logger.info("Init elector impl...");

            dbDataSource.init(false);

            ElectorWorkerThread electorWorkerThread = new ElectorWorkerThread();
            fixedThreadPool.execute(electorWorkerThread);

            DBMonitorTask dbMonitorTask = new DBMonitorTask(electorConfig, dbDataSource);
            fixedThreadPool.execute(dbMonitorTask);
        } catch (Exception exception) {
            logger.error("Exception :{}", exception.getMessage());
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
                logger.error("Exception :{}", exception.getMessage());
            }

        try {
            TimeUnit.SECONDS.sleep(40L);
        } catch (Exception exception) {
            logger.error("Exception :{}", exception.getMessage());
        }
    }

    /**
     * Replace leader
     *
     * @param newLeaderId
     */
    public void replaceLeader(String newLeaderId) {
        sleepTime = (electorConfig.getTryToBeLeaderInterval() * 2);
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
    public void canElector(boolean canElector) {
        this.canElector = canElector;
    }

    /**
     * Rebuild elector DBSource
     *
     * @return
     */
    public boolean reBuildElectorDBSource() {
        canElector(false);
        try {
            releaseLeader();
            dbDataSource.close();
            dbDataSource.init(true);
            canElector(true);
        } catch (Exception exception) {
            logger.error("Exception :{}", exception.getMessage());
            return false;
        }
        return true;
    }

    /**
     * close
     *
     * @return
     */
    public boolean close() {
        return false;
    }

    class ElectorWorkerThread implements Runnable {

        Random random;

        ElectorWorkerThread() {
            this.random = new Random();
        }

        public void run() {
            while (true) {
                if (canElector) {
                    dbDataSource.leaderSelector();
                }

                String leaderId = dbDataSource.getCurrentLeader();
                if (!StringUtils.isEmpty(leaderId)) {
                    if (electorConfig.getLeaderId().equals(leaderId)) {
                        if (!isLeader
                                && electorConfig.getElectorChangeListener() != null) {
                            electorConfig.getElectorChangeListener().leaderChanged(true);
                        }

                        isLeader = true;
                        sleepTime = (long) electorConfig.getTryToBeLeaderInterval();
                    } else {
                        if (isLeader
                                && electorConfig.getElectorChangeListener() != null) {
                            electorConfig.getElectorChangeListener().leaderChanged(false);
                        }

                        isLeader = false;
                        sleepTime = (electorConfig.getTryToBeLeaderInterval() * 2L
                                + random.nextInt(5));
                    }
                }

                try {
                    TimeUnit.SECONDS.sleep(sleepTime);
                } catch (Exception exception) {
                    logger.error("Exception :{}", exception.getMessage());
                }
            }
        }
    }
}