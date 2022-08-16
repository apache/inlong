/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.inlong.sort.protocol.constant;

/**
 * Doris options constant
 */
public class DorisConstant {

    /**
     * 'connector' = 'doris'
     */
    public static final String CONNECTOR = "connector";

    /**
     * Doris FE http address, support multiple addresses, separated by commas
     */
    public static final String FE_NODES = "fenodes";

    /**
     * Doris table identifier, eg, db1.tbl1
     */
    public static final String TABLE_IDENTIFIER = "table.identifier";

    /**
     * Doris username
     */
    public static final String USERNAME = "username";

    /**
     * Doris password
     */
    public static final String PASSWORD = "password";

    /**
     * List of column names in the Doris table, separated by commas
     */
    public static final String DORIS_READ_FIELD = "doris.read.field";

    /**
     * Filter expression of the query, which is transparently transmitted to Doris.
     * Doris uses this expression to complete source-side data filtering
     */
    public static final String DORIS_FILTER_QUERY = "doris.filter.query";

    /**
     * Whether to enable deletion.
     * This option requires Doris table to enable batch delete function (0.15+ version is enabled by default),
     * and only supports Uniq model.
     *
     * default value : true
     */
    public static final String SINK_ENABLE_DELETE = "sink.enable-delete";

    /**
     * In the 2pc scenario, the number of retries after the commit phase fails.
     *
     * default value: 1
     */
    public static final String SINK_MAX_RETRIES = "sink.max-retries";

    /**
     * Write data cache buffer size, in bytes. It is not recommended to modify, the default configuration is sufficient.
     *
     * default value: 1048576(1MB)
     */
    public static final String SINK_BUFFER_SIZE = "sink.buffer-size";

    /**
     * The number of write data cache buffers, it is not recommended to modify, the default configuration is sufficient.
     *
     * default value: 3
     */
    public static final String SINK_BUFFER_COUNT = "sink.buffer-count";

}
