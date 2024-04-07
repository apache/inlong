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

package elector.task;

import elector.api.ElectorConfig;
import elector.impl.DBDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class DBMonitorTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DBMonitorTask.class);
    private ElectorConfig electorConfig;
    private DBDataSource dbDataSource;
    private int dbClosedTimes = 0;
    private boolean replaced = true;

    public DBMonitorTask(ElectorConfig electorConfig, DBDataSource dbDataSource) {
        this.electorConfig = electorConfig;
        this.dbDataSource = dbDataSource;
    }

    public void run() {
        try {
            while (true) {
                logger.info("## DBMonitorTask run once");
                TimeUnit.SECONDS.sleep(electorConfig.getDbMonitorRunInterval());

                if (!(electorConfig.isUseDefaultLeader()))
                    break;
            }
            if (dbDataSource.isDBDataSourceClosed()) {
                dbClosedTimes += 1;
                logger.info("## db closed true :" + dbClosedTimes);
            } else {
                dbClosedTimes = 0;
            }

            if (dbClosedTimes >= 3) {
                logger.warn("DBMonitorTask : fail  begin use default leader continue fail times:" + dbClosedTimes);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}