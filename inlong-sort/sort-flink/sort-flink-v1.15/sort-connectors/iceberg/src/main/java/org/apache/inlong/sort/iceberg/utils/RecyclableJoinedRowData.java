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

package org.apache.inlong.sort.iceberg.utils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import java.util.Objects;

/**
 * JoinedRowData that support recycle two member RowData by provide get() and set() interfaces.
 */
@PublicEvolving
public class RecyclableJoinedRowData implements RowData {

    private RowKind rowKind = RowKind.INSERT;
    private RowData physicalRowData;
    private RowData metaRowData;
    private long dataTime;

    public RecyclableJoinedRowData() {
    }

    public RecyclableJoinedRowData(int physicalSize, int metaSize) {
        physicalRowData = new GenericRowData(physicalSize);
        metaRowData = new GenericRowData(metaSize);
    }

    public RecyclableJoinedRowData replace(RowKind rowKind, RowData physicalRowData, RowData metaRowData,
            long dataTime) {
        this.rowKind = rowKind;
        this.physicalRowData = physicalRowData;
        this.metaRowData = metaRowData;
        this.dataTime = dataTime;
        return this;
    }

    public RowData getPhysicalRowData() {
        return physicalRowData;
    }

    public RowData getMetaRowData() {
        return metaRowData;
    }

    public long getDataTime() {
        return dataTime;
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public int getArity() {
        return physicalRowData.getArity() + metaRowData.getArity();
    }

    @Override
    public RowKind getRowKind() {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind kind) {
        this.rowKind = kind;
    }

    @Override
    public boolean isNullAt(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.isNullAt(pos);
        } else {
            return metaRowData.isNullAt(pos - physicalRowData.getArity());
        }
    }

    @Override
    public boolean getBoolean(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getBoolean(pos);
        } else {
            return metaRowData.getBoolean(pos - physicalRowData.getArity());
        }
    }

    @Override
    public byte getByte(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getByte(pos);
        } else {
            return metaRowData.getByte(pos - physicalRowData.getArity());
        }
    }

    @Override
    public short getShort(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getShort(pos);
        } else {
            return metaRowData.getShort(pos - physicalRowData.getArity());
        }
    }

    @Override
    public int getInt(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getInt(pos);
        } else {
            return metaRowData.getInt(pos - physicalRowData.getArity());
        }
    }

    @Override
    public long getLong(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getLong(pos);
        } else {
            return metaRowData.getLong(pos - physicalRowData.getArity());
        }
    }

    @Override
    public float getFloat(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getFloat(pos);
        } else {
            return metaRowData.getFloat(pos - physicalRowData.getArity());
        }
    }

    @Override
    public double getDouble(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getDouble(pos);
        } else {
            return metaRowData.getDouble(pos - physicalRowData.getArity());
        }
    }

    @Override
    public StringData getString(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getString(pos);
        } else {
            return metaRowData.getString(pos - physicalRowData.getArity());
        }
    }

    @Override
    public DecimalData getDecimal(int pos, int precision, int scale) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getDecimal(pos, precision, scale);
        } else {
            return metaRowData.getDecimal(pos - physicalRowData.getArity(), precision, scale);
        }
    }

    @Override
    public TimestampData getTimestamp(int pos, int precision) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getTimestamp(pos, precision);
        } else {
            return metaRowData.getTimestamp(pos - physicalRowData.getArity(), precision);
        }
    }

    @Override
    public <T> RawValueData<T> getRawValue(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getRawValue(pos);
        } else {
            return metaRowData.getRawValue(pos - physicalRowData.getArity());
        }
    }

    @Override
    public byte[] getBinary(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getBinary(pos);
        } else {
            return metaRowData.getBinary(pos - physicalRowData.getArity());
        }
    }

    @Override
    public ArrayData getArray(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getArray(pos);
        } else {
            return metaRowData.getArray(pos - physicalRowData.getArity());
        }
    }

    @Override
    public MapData getMap(int pos) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getMap(pos);
        } else {
            return metaRowData.getMap(pos - physicalRowData.getArity());
        }
    }

    @Override
    public RowData getRow(int pos, int numFields) {
        if (pos < physicalRowData.getArity()) {
            return physicalRowData.getRow(pos, numFields);
        } else {
            return metaRowData.getRow(pos - physicalRowData.getArity(), numFields);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecyclableJoinedRowData that = (RecyclableJoinedRowData) o;
        return Objects.equals(rowKind, that.rowKind)
                && Objects.equals(this.physicalRowData, that.physicalRowData)
                && Objects.equals(this.metaRowData, that.metaRowData);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowKind, physicalRowData, metaRowData);
    }

    @Override
    public String toString() {
        return rowKind.shortString() + "{" + "physicalRowData=" + physicalRowData + ", metaRowData=" + metaRowData
                + '}';
    }
}
