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

package org.apache.inlong.agent.plugin.utils;

import com.ververica.cdc.debezium.internal.SchemaRecord;
import com.ververica.cdc.debezium.utils.DatabaseHistoryUtil;
import io.debezium.config.Configuration;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

/**
 * @description
 * @date: 2022/5/9
 */
public class InLongDatabaseHistory extends AbstractDatabaseHistory {

    public static final String DATABASE_HISTORY_INSTANCE_NAME = "database.history.instance.name";
    private ConcurrentLinkedQueue<SchemaRecord> schemaRecords;
    private String instanceName;

    public InLongDatabaseHistory() {
    }

    private ConcurrentLinkedQueue<SchemaRecord> getRegisteredHistoryRecord(String instanceName) {
        Collection<SchemaRecord> historyRecords = DatabaseHistoryUtil.retrieveHistory(instanceName);
        return new ConcurrentLinkedQueue(historyRecords);
    }

    public void configure(Configuration config, HistoryRecordComparator comparator, DatabaseHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        this.instanceName = config.getString("database.history.instance.name");
        this.schemaRecords = this.getRegisteredHistoryRecord(this.instanceName);
        DatabaseHistoryUtil.registerHistory(this.instanceName, this.schemaRecords);
    }

    public void stop() {
        super.stop();
        DatabaseHistoryUtil.removeHistory(this.instanceName);
    }

    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        this.schemaRecords.add(new SchemaRecord(record));
    }

    protected void recoverRecords(Consumer<HistoryRecord> records) {
        this.schemaRecords.stream().map(SchemaRecord::getHistoryRecord).forEach(records);
    }

    public boolean exists() {
        return !this.schemaRecords.isEmpty();
    }

    public boolean storageExists() {
        return true;
    }

    public String toString() {
        return "Flink Database History";
    }

    public static boolean isCompatible(Collection<SchemaRecord> records) {
        Iterator var1 = records.iterator();
        if (var1.hasNext()) {
            SchemaRecord record = (SchemaRecord) var1.next();
            if (!record.isHistoryRecord()) {
                return false;
            }
        }

        return true;
    }
}
