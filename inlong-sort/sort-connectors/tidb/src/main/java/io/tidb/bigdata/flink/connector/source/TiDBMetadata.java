/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.flink.connector.source;

import io.tidb.bigdata.flink.format.cdc.CDCMetadata;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.tikv.common.meta.TiTimestamp;

public enum TiDBMetadata {
  COMMIT_VERSION("commit_version",
      DataTypes.BIGINT().notNull(),
      TiDBMetadata::commitVersion,
      CDCMetadata.COMMIT_VERSION),
  COMMIT_TIMESTAMP("commit_timestamp",
      DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull(),
      TiDBMetadata::commitMs,
      CDCMetadata.COMMIT_TIMESTAMP);

  private static final TiDBMetadata[] EMPTY = new TiDBMetadata[0];

  private final String key;
  private final DataType type;
  private final Function<TiTimestamp, Object> extractor;
  private final CDCMetadata craft;

  TiDBMetadata(final String key, final DataType type,
      Function<TiTimestamp, Object> extractor,
      CDCMetadata craft) {
    this.key = key;
    this.type = type;
    this.extractor = extractor;
    this.craft = craft;
  }

  public String getKey() {
    return key;
  }

  public DataType getType() {
    return type;
  }

  public <T> T extract(TiTimestamp ts) {
    return (T) extractor.apply(ts);
  }

  public CDCMetadata toCraft() {
    return craft;
  }

  public DataTypes.Field toField() {
    return DataTypes.FIELD(key, type);
  }

  private static long commitVersion(final TiTimestamp ts) {
    return ts.getVersion();
  }

  private static TimestampData commitMs(final TiTimestamp ts) {
    return TimestampData.fromEpochMillis(ts.getPhysical());
  }

  public static TiDBMetadata[] toMetadata(Collection<String> key) {
    return key.stream()
        .map(String::toUpperCase)
        .map(TiDBMetadata::valueOf)
        .toArray(TiDBMetadata[]::new);
  }

  public static Map<String, DataType> listReadableMetadata() {
    return Stream.of(TiDBMetadata.values())
        .collect(Collectors.toMap(TiDBMetadata::getKey, TiDBMetadata::getType));
  }

  public static TiDBMetadata[] empty() {
    return EMPTY;
  }
}