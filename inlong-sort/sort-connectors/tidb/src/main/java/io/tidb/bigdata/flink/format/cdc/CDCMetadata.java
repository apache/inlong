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

package io.tidb.bigdata.flink.format.cdc;

import io.tidb.bigdata.cdc.Event;
import io.tidb.bigdata.cdc.json.jackson.JacksonFactory;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

public enum CDCMetadata {

    SCHEMA("schema", DataTypes.STRING().nullable(), CDCMetadata::schema),
    TABLE("table", DataTypes.STRING().nullable(), CDCMetadata::table),
    COMMIT_VERSION("commit_version", DataTypes.BIGINT().notNull(), Event::getTs),
    COMMIT_TIMESTAMP("commit_timestamp",
            DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
            CDCMetadata::commitMs),
    TYPE("type", DataTypes.STRING().notNull(), CDCMetadata::typeName),
    TYPE_CODE("type_code", DataTypes.INT().notNull(), CDCMetadata::typeCode),
    KEY("key", DataTypes.STRING().nullable(), CDCMetadata::key),
    VALUE("value", DataTypes.STRING().nullable(), CDCMetadata::value);

    private static final CDCMetadata[] EMPTY = new CDCMetadata[0];
    private static final JacksonFactory flinkShadedJackson =
            JacksonFactory.create("org.apache.flink.shaded.jackson2");
    private final String key;
    private final DataType type;
    private final Function<Event, Object> extractor;

    CDCMetadata(final String key, final DataType type, Function<Event, Object> extractor) {
        this.key = key;
        this.type = type;
        this.extractor = extractor;
    }

    private static Integer typeCode(final Event event) {
        return event.getType().code();
    }

    private static StringData typeName(final Event event) {
        return StringData.fromString(event.getType().name());
    }

    private static TimestampData commitMs(final Event event) {
        return TimestampData.fromEpochMillis(event.getTimestamp());
    }

    private static StringData schema(final Event event) {
        return StringData.fromString(event.getSchema());
    }

    private static StringData table(final Event event) {
        return StringData.fromString(event.getTable());
    }

    private static StringData key(final Event event) {
        return StringData.fromString(event.getKey().toJson(flinkShadedJackson));
    }

    private static StringData value(final Event event) {
        return StringData.fromString(event.getValue().toJson(flinkShadedJackson));
    }

    public String getKey() {
        return key;
    }

    public DataType getType() {
        return type;
    }

    public <T> T extract(Event event) {
        return (T) extractor.apply(event);
    }

    public DataTypes.Field toField() {
        return DataTypes.FIELD(key, type);
    }

    public static CDCMetadata[] toMetadata(Collection<String> key) {
        return key.stream()
                .map(String::toUpperCase)
                .map(CDCMetadata::valueOf)
                .toArray(CDCMetadata[]::new);
    }

    public static Map<String, DataType> listReadableMetadata() {
        return Stream.of(CDCMetadata.values())
                .collect(Collectors.toMap(CDCMetadata::getKey, CDCMetadata::getType));
    }

    public static CDCMetadata[] empty() {
        return EMPTY;
    }

    public static CDCMetadata[] notNull(CDCMetadata[] input) {
        if (input == null) {
            return empty();
        } else {
            return input;
        }
    }
}