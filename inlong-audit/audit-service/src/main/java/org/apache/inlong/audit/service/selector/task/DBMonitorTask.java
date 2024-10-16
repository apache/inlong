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

package org.apache.inlong.audit.service.selector.task;

import org.apache.inlong.audit.service.selector.api.SelectorConfig;
import org.apache.inlong.audit.service.selector.impl.DBDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * DB monitor task
 */
public class DBMonitorTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBMonitorTask.class);
    private final SelectorConfig selectorConfig;
    private final DBDataSource dbDataSource;
    private int dbClosedTimes = 0;
    private final boolean replaced = true;

    public DBMonitorTask(SelectorConfig selectorConfig, DBDataSource dbDataSource) {
        this.selectorConfig = selectorConfig;
        this.dbDataSource = dbDataSource;
    }

    public void run() {
        try {
            while (true) {
                LOGGER.info("DB monitor task run once");
                TimeUnit.SECONDS.sleep(selectorConfig.getDbMonitorRunInterval());

                if (!(selectorConfig.isUseDefaultLeader()))
                    break;
            }
            if (dbDataSource.isDBDataSourceClosed()) {
                dbClosedTimes += 1;
                LOGGER.info("DB closed times :{}", dbClosedTimes);
            } else {
                dbClosedTimes = 0;
            }
        } catch (Exception e) {
            LOGGER.error("DB monitor task has exception {}", e.getMessage());
        }
    }
}