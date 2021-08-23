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

package org.apache.inlong.sort.flink;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Objects;
import org.apache.flink.types.Row;

/**
 * Represents a row with data flow id attached.
 */
public class Record implements Serializable {

    private static final long serialVersionUID = 403818036307888751L;

    private long dataflowId;

    // Event time
    private long timestampMillis;

    private Row row;

    /**
     * Just to satisfy the requirement of Flink Pojo definition.
     */
    public Record() {

    }

    public Record(long dataflowId, long timestampMillis, Row row) {
        this.dataflowId = dataflowId;
        this.timestampMillis = timestampMillis;
        this.row = checkNotNull(row);
    }

    public long getDataflowId() {
        return dataflowId;
    }

    public void setDataflowId(long dataflowId) {
        this.dataflowId = dataflowId;
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    public void setTimestampMillis(long timestampMillis) {
        this.timestampMillis = timestampMillis;
    }

    public Row getRow() {
        return row;
    }

    public void setRow(Row row) {
        this.row = row;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Record that = (Record) o;
        return dataflowId == that.dataflowId
                && Objects.equals(row, that.row)
                && timestampMillis == that.timestampMillis;
    }
}
