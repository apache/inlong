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

package org.apache.inlong.sort.cdc.mysql.source.split;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.util.Map;

/**
 * The split to describe a split of MySql metric.
 */
public class MySqlMetricSplit extends MySqlSplit {

    private Long numRecordsIn = 0L;

    private Long numBytesIn = 0L;

    public Long getNumRecordsIn() {
        return numRecordsIn;
    }

    public void setNumRecordsIn(Long numRecordsIn) {
        this.numRecordsIn = numRecordsIn;
    }

    public Long getNumBytesIn() {
        return numBytesIn;
    }

    public void setNumBytesIn(Long numBytesIn) {
        this.numBytesIn = numBytesIn;
    }

    public MySqlMetricSplit(String splitId) {
        super(splitId);
    }

    public MySqlMetricSplit(Long numBytesIn, Long numRecordsIn) {
        this("");
        this.numBytesIn = numBytesIn;
        this.numRecordsIn = numRecordsIn;
    }

    public void setMetricData(long count, long byteNum) {
        numRecordsIn = numRecordsIn + count;
        numBytesIn = numBytesIn + byteNum;
    }

    @Override
    public Map<TableId, TableChange> getTableSchemas() {
        return null;
    }

    @Override
    public String toString() {
        return "MysqlMetricSplit{"
                + "numRecordsIn=" + numRecordsIn
                + ", numBytesIn=" + numBytesIn
                + '}';
    }
}
