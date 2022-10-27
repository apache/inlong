/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.inlong.sort.base.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy.ALERT_WITH_IGNORE;
import static org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy.LOG_WITH_IGNORE;
import static org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy.TRY_IT_BEST;

/**
 * MultipleSinkOption collect all parameters used for multiple sink.
 */
public class MultipleSinkOption implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MultipleSinkOption.class);

    private String format;

    private SchemaUpdateExceptionPolicy schemaUpdatePolicy;

    private String databasePattern;

    private String tablePattern;

    public MultipleSinkOption(String format,
            SchemaUpdateExceptionPolicy schemaUpdatePolicy,
            String databasePattern,
            String tablePattern) {
        this.format = format;
        this.schemaUpdatePolicy = schemaUpdatePolicy;
        this.databasePattern = databasePattern;
        this.tablePattern = tablePattern;
    }

    public String getFormat() {
        return format;
    }

    public SchemaUpdateExceptionPolicy getSchemaUpdatePolicy() {
        return schemaUpdatePolicy;
    }

    public String getDatabasePattern() {
        return databasePattern;
    }

    public String getTablePattern() {
        return tablePattern;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String format;
        private SchemaUpdateExceptionPolicy schemaUpdatePolicy;
        private String databasePattern;
        private String tablePattern;

        public MultipleSinkOption.Builder withFormat(String format) {
            this.format = format;
            return this;
        }

        public MultipleSinkOption.Builder withSchemaUpdatePolicy(SchemaUpdateExceptionPolicy schemaUpdatePolicy) {
            this.schemaUpdatePolicy = schemaUpdatePolicy;
            return this;
        }

        public MultipleSinkOption.Builder withDatabasePattern(String databasePattern) {
            this.databasePattern = databasePattern;
            return this;
        }

        public MultipleSinkOption.Builder withTablePattern(String tablePattern) {
            this.tablePattern = tablePattern;
            return this;
        }

        public MultipleSinkOption build() {
            return new MultipleSinkOption(format, schemaUpdatePolicy, databasePattern, tablePattern);
        }
    }

    public static boolean canHandleWithSchemaUpdate(String tableName,
            TableChange tableChange, SchemaUpdateExceptionPolicy policy) {
        if (TRY_IT_BEST.equals(policy)) {
            return true;
        } else if (LOG_WITH_IGNORE.equals(policy) || ALERT_WITH_IGNORE.equals(policy)) {
            LOG.warn("Ignore table {} schema change: {}.", tableName, tableChange);
            return false;
        }

        throw new UnsupportedOperationException(
                String.format("Unsupported table %s schema change: %s.", tableName, tableChange));
    }
}
