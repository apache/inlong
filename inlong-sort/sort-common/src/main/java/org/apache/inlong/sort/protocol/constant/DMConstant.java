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

package org.apache.inlong.sort.protocol.constant;

import lombok.Getter;

/**
 * Dameng options constant
 */
public class DMConstant {

    /**
     * The key of flink connector defined in flink table
     */
    public static final String CONNECTOR = "connector";
    /**
     * Specify what flink connector to use for extract data from Dameng database, here should be 'dm-cdc'
     */
    public static final String DM_CDC = "dm-cdc-inlong";
    /**
     * Database name of the Dameng server to monitor
     */
    public static final String DATABASE_NAME = "database-name";
    /**
     * IP address or hostname of the Dameng database server
     */
    public static final String HOSTNAME = "hostname";
    /**
     * Integer port number of the Dameng database server.
     */
    public static final String PORT = "port";
    /**
     * Name of the Dameng database to use when connecting to the Dameng database server
     */
    public static final String USERNAME = "username";
    /**
     * Password to use when connecting to the Dameng database server
     */
    public static final String PASSWORD = "password";
    /**
     * Table name of the Dameng database to monitor
     */
    public static final String TABLE_NAME = "table-name";
    /**
     * Schema name of the Dameng database to monitor
     */
    public static final String SCHEMA_NAME = "schema-name";
    /**
     * <p>The mining strategy controls how Dameng LogMiner builds
     * and uses a given data dictionary for resolving table and column ids to names.</p>
     * <p>redo_log_catalog - Writes the data dictionary to the online redo logs
     * causing more archive logs to be generated over time.
     * This also enables tracking DDL changes against captured tables,
     * so if the schema changes frequently this is the ideal choice.</p>
     * <p>online_catalog - Uses the database’s current data dictionary to resolve object ids
     * and does not write any extra information to the online redo logs.
     * This allows LogMiner to mine substantially faster but at the expense that DDL changes cannot be tracked.
     * If the captured table(s) schema changes infrequently or never, this is the ideal choice.</p>
     */
    public static final String LOG_MINING_STRATEGY = "debezium.log.mining.strategy";
    /**
     * If true,CONTINUOUS_MINE option will be added to the log mining session.
     * This will manage log files switches seamlessly.
     */
    public static final String LOG_MINING_CONTINUOUS_MINE = "debezium.log.mining.continuous.mine";
    /**
     * Deprecated: Case insensitive table names;set to 'true' for Dameng 11g,'false'(default) otherwise.
     */
    public static final String TABLENAME_CASE_INSENSITIVE = "debezium.database.tablename.case.insensitive";
    /**
     * The key of ${@link ScanStartUpMode}
     */
    public static final String SCAN_STARTUP_MODE = "scan.startup.mode";

    /**
     * Optional startup mode for Dameng CDC consumer,
     * valid enumerations are "initial" and "latest-offset".
     * Please see Startup Reading Positionsection for more detailed information.
     */
    @Getter
    public enum ScanStartUpMode {

        /**
         * Performs an initial snapshot on the monitored database tables upon first startup,
         * and continue to read the latest binlog.
         */
        INITIAL("initial"),
        /**
         * Never to perform a snapshot on the monitored database tables upon first startup,
         * just read from the change since the connector was started.
         */
        LATEST_OFFSET("latest-offset");

        final String value;

        ScanStartUpMode(String value) {
            this.value = value;
        }

        public static ScanStartUpMode forName(String name) {
            for (ScanStartUpMode dataType : ScanStartUpMode.values()) {
                if (dataType.getValue().equals(name)) {
                    return dataType;
                }
            }
            throw new IllegalArgumentException(String.format("Unsupport ScanStartUpMode for Dameng source:%s", name));
        }
    }

}
