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

package org.apache.inlong.sort.cdc.dm.table;

import org.apache.inlong.sort.protocol.ddl.enums.OperationType;
import java.io.Serializable;

import java.util.HashMap;
import java.util.Map;

/** An internal data structure representing record of DM. */
public class DMRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final SourceInfo sourceInfo;
    private final boolean isSnapshotRecord;

    // four fields represents a rowdata
    private final OperationType opt;
    private final Map<String, Object> jdbcFields;
    private final Map<String, Object> logMessageFieldsBefore;
    private final Map<String, Object> logMessageFieldsAfter;

    public DMRecord(
            SourceInfo sourceInfo,
            OperationType opt,
            Map<String, Object> jdbcFields) {
        this.sourceInfo = sourceInfo;
        this.isSnapshotRecord = true;
        this.jdbcFields = jdbcFields;
        this.opt = opt;
        this.logMessageFieldsBefore = new HashMap<>();
        this.logMessageFieldsAfter = new HashMap<>();
    }

    public DMRecord(
            SourceInfo sourceInfo,
            OperationType opt,
            Map<String, Object> logMessageFieldsBefore,
            Map<String, Object> logMessageFieldsAfter) {
        this.sourceInfo = sourceInfo;
        this.isSnapshotRecord = false;
        this.jdbcFields = new HashMap<>();
        this.opt = opt;
        this.logMessageFieldsBefore = logMessageFieldsBefore;
        this.logMessageFieldsAfter = logMessageFieldsAfter;
    }

    public SourceInfo getSourceInfo() {
        return sourceInfo;
    }

    public boolean isSnapshotRecord() {
        return isSnapshotRecord;
    }

    public Map<String, Object> getJdbcFields() {
        return jdbcFields;
    }

    public OperationType getOpt() {
        return opt;
    }

    public Map<String, Object> getLogMessageFieldsBefore() {
        return logMessageFieldsBefore;
    }

    public Map<String, Object> getLogMessageFieldsAfter() {
        return logMessageFieldsAfter;
    }

    @Override
    public String toString() {
        return "DMRecord{"
                + "sourceInfo="
                + sourceInfo
                + ", isSnapshotRecord='"
                + isSnapshotRecord
                + '\''
                + ", jdbcFields="
                + jdbcFields
                + ", logMessageFieldsBefore="
                + logMessageFieldsBefore
                + ", logMessageFieldsAfter="
                + logMessageFieldsAfter
                + ", opt="
                + opt
                + '}';
    }

    /** Information about the source of record. */
    public static class SourceInfo implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String database;
        private final String schema;
        private final String table;
        private final long scn;

        public SourceInfo(String database, String schema, String table, long scn) {
            this.database = database;
            this.schema = schema;
            this.table = table;
            this.scn = scn;
        }

        public String getDatabase() {
            return database;
        }

        public String getTable() {
            return table;
        }

        public String getSchema() {
            return schema;
        }

        public long getSCN() {
            return scn;
        }

        @Override
        public String toString() {
            return "sourceInfo{"
                    + getSCN() + ","
                    + getDatabase() + ","
                    + getSchema() + ","
                    + getTable()
                    + '}';
        }
    }
}
